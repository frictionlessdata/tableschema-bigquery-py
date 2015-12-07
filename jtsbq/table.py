# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import csv
import json
import time
from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from tabulator import topen, processors
from jsontableschema.model import SchemaModel
from oauth2client.client import SignedJwtAssertionCredentials


class Table(object):
    """BigQuery table representation.
    """

    # Public

    SCOPE = ['https://www.googleapis.com/auth/bigquery']
    DOWNLOAD_TYPES = {
        'STRING': 'string',
        'INTEGER': 'integer',
        'FLOAT': 'number',
        'BOOLEAN': 'boolean',
        'TIMESTAMP': 'datetime',
    }
    UPLOAD_TYPES = {
        'string': 'STRING',
        'number': 'FLOAT',
    }

    def __init__(self, client_email, private_key,
                 project_id, dataset_id, table_id):
        self.__client_email = client_email
        self.__private_key = private_key
        self.__project_id = project_id
        self.__dataset_id = dataset_id
        self.__table_id = table_id

    def download(self, schema_path, data_path):
        """Download table's schema+data

        Directory of the files has to be existent.

        Parameters
        ----------
        schema_path (str):
            Path to schema (json) file to be saved.
        data_path (str):
            Path to data (csv) file to be saved.

        """

        # Convert schema
        fields = []
        for field in self.__schema['fields']:
            try:
                ftype = self.DOWNLOAD_TYPES[field['type']]
            except KeyError:
                message = 'Type %s is not supported' % field['type']
                raise TypeError(message)
            # TODO: fix required
            fields.append({
                'name': field['name'],
                'type': ftype,
            })
        schema = {'fields': fields}

        # Prepare headers and rows
        model = SchemaModel(schema)
        headers = model.headers
        rows = []
        for row in self.__rows:
            row = tuple(model.convert_row(*row))
            rows.append(row)

        # Write files on disk
        with io.open(schema_path, mode='w', encoding='utf-8') as file:
            json.dump(schema, file, indent=4)
        with io.open(data_path, mode='w', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            # TODO: remove additional loop
            for row in rows:
                writer.writerow(row)

    def upload(self, schema_path, data_path, **options):
        """Upload schema+data to BigQuery.
        """

        # Read schema
        with io.open(schema_path, encoding='utf-8') as file:
            schema = json.load(file)

        # Read data
        with topen(data_path, **options) as table:
            # TODO: add header row config
            table.add_processor(processors.Headers())
            table.add_processor(processors.Schema(schema))
            data = table.read()  # noqa

        # Convert schema
        model = SchemaModel(schema)
        fields = []
        for field in model.fields:
            try:
                ftype = self.UPLOAD_TYPES[field['type']]
            except KeyError:
                message = 'Type %s is not supported' % field['type']
                raise TypeError(message)
            fields.append({
                'name': field['name'],
                'type': ftype,
            })
        schema = {'fields': fields}

        # Prepare job body
        body = {
            'configuration': {
                'load': {
                    'schema':  schema,
                    'destinationTable': {
                        'projectId': self.__project_id,
                        'datasetId': self.__dataset_id,
                        'tableId': self.__table_id
                    },
                    'sourceFormat': 'CSV',
                    "skipLeadingRows": 1,
                }
            }
        }

        # Prepare media body
        # http://developers.google.com/api-client-library/python/guide/media_upload
        media_body = MediaFileUpload(
                data_path, mimetype='application/octet-stream')

        # Post to the jobs resource
        job = self.__bigquery.jobs().insert(
            projectId=self.__project_id,
            body=body,
            media_body=media_body).execute()
        status = self.__bigquery.jobs().get(
            projectId=job['jobReference']['projectId'],
            jobId=job['jobReference']['jobId'])

        # Poll the job until it finishes.
        while True:
            result = status.execute(num_retries=2)
            if result['status']['state'] == 'DONE':
                if result['status'].get('errors'):
                    message = '\n'.join(
                            e['message'] for e in result['status']['errors'])
                    raise RuntimeError(message)
                break
            time.sleep(1)

    # Private

    @property
    def __rows(self):
        """Return list of rows (tuples).
        """

        # Return from cache
        if hasattr(self, '___rows'):
            return self.___rows

        # Prepare rows
        template = 'SELECT * FROM [{project_id}:{dataset_id}.{table_id}];'
        query = template.format(
                project_id=self.__project_id,
                dataset_id=self.__dataset_id,
                table_id=self.__table_id)
        response = self.__bigquery.jobs().query(
            projectId=self.__project_id,
            body={'query': query}).execute()
        self.___rows = []
        for row in response['rows']:
            self.___rows.append(tuple(field['v'] for field in row['f']))

        return self.___rows

    @property
    def __schema(self):
        """Return schema dict.
        """

        # Return from cache
        if hasattr(self, '___schema'):
            return self.___schema

        # Prepare schema
        tables = self.__bigquery.tables()
        table = tables.get(
                projectId=self.__project_id,
                datasetId=self.__dataset_id,
                tableId=self.__table_id).execute()
        self.___schema = table['schema']

        return self.___schema

    @property
    def __bigquery(self):
        """Return bigquery service object.
        """

        # Return from cache
        if hasattr(self, '___bigquery'):
            return self.___bigquery

        # Prepare service
        credentials = SignedJwtAssertionCredentials(
                self.__client_email, self.__private_key, self.SCOPE)
        self.___bigquery = build('bigquery', 'v2', credentials=credentials)

        return self.___bigquery
