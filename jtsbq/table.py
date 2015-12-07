# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import csv
import six
import json
import time
from apiclient.discovery import build
from apiclient.http import MediaIoBaseUpload
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
        mode = 'w'
        newline = ''
        encoding = 'utf-8'
        if six.PY2:
            mode = 'wb'
            newline = None
            encoding = None
        with io.open(schema_path,
                     mode=mode,
                     encoding=encoding) as file:
            json.dump(schema, file, indent=4)
        with io.open(data_path,
                     mode=mode,
                     newline=newline,
                     encoding=encoding) as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            # TODO: remove additional loop
            for row in rows:
                writer.writerow(row)

    def upload(self, schema_path, data_path, **options):
        """Upload schema+data to BigQuery.

        Parameters
        ----------
        schema_path (str):
            Path to schema (json) file to be uploaded.
        data_path (str):
            Path to data (csv) file to be uploaded.
        options (dict):
            Tabulator options.

        """

        # Read schema
        with io.open(schema_path, encoding='utf-8') as file:
            schema = json.load(file)

        # Read data
        bytes = io.BufferedRandom(io.BytesIO())
        class Stream: #noqa
            def write(self, string):
                bytes.write(string.encode('utf-8'))
        stream = Stream()
        with topen(data_path, **options) as table:
            # TODO: add header row config?
            table.add_processor(processors.Headers())
            table.add_processor(processors.Schema(schema))
            writer = csv.writer(stream)
            for row in table.readrow():
                writer.writerow(row)
        bytes.seek(0)

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
                }
            }
        }

        # Prepare job media body
        mimetype = 'application/octet-stream'
        media_body = MediaIoBaseUpload(bytes, mimetype=mimetype)

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
                    errors = result['status']['errors']
                    message = '\n'.join(error['message'] for error in errors)
                    raise RuntimeError(message)
                break
            time.sleep(1)

        # Reset rows cache
        self.__rows_cache = None

    # Private

    @property
    def __rows(self):
        """Return list of rows (tuples).
        """

        # Return from cache
        if getattr(self, '__rows_cache', None) is not None:
            return self.__rows_cache

        # Prepare rows
        template = 'SELECT * FROM [{project_id}:{dataset_id}.{table_id}];'
        query = template.format(
                project_id=self.__project_id,
                dataset_id=self.__dataset_id,
                table_id=self.__table_id)
        response = self.__bigquery.jobs().query(
            projectId=self.__project_id,
            body={'query': query}).execute()
        self.__rows_cache = []
        for row in response['rows']:
            self.__rows_cache.append(tuple(field['v'] for field in row['f']))

        return self.__rows_cache

    @property
    def __schema(self):
        """Return schema dict.
        """

        # Return from cache
        if getattr(self, '__schema_cache', None) is not None:
            return self.__schema_cache

        # Prepare schema
        tables = self.__bigquery.tables()
        table = tables.get(
                projectId=self.__project_id,
                datasetId=self.__dataset_id,
                tableId=self.__table_id).execute()
        self.__schema_cache = table['schema']

        return self.__schema_cache

    @property
    def __bigquery(self):
        """Return bigquery service object.
        """

        # Return from cache
        if getattr(self, '__bigquery_cache', None) is not None:
            return self.__bigquery_cache

        # Prepare service
        credentials = SignedJwtAssertionCredentials(
                self.__client_email, self.__private_key, self.SCOPE)
        self.__bigquery_cache = build(
                'bigquery', 'v2', credentials=credentials)

        return self.__bigquery_cache
