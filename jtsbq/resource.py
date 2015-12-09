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
from apiclient.http import MediaIoBaseUpload
from tabulator import topen, processors
from jsontableschema.model import SchemaModel

from . import schema as schema_module
from .table import Table


# Module API

class Resource(object):
    """Data resource representation.
    """

    # Public

    def __init__(self, service, project_id, dataset_id, table_id,
                 schema=None):

        # Convert schema
        if schema is not None:
            schema = schema_module.resource2table(schema)

        # Create table
        self.__table = Table(
                service=service,
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id,
                schema=schema)

    @property
    def schema(self):
        """Return schema.
        """

        # Create cache
        if not hasattr(self, '__schema'):

            # Get and convert schema
            schema = self.__table.schema
            schema = schema_module.table2resource(schema)
            self.__schema = schema

        return self.__schema

    def get_data(self):
        """Return data generator.
        """

        # Get data and model
        data = self.__table.get_data()
        model = SchemaModel(self.schema)

        # Yield converted data
        for row in data:
            row = tuple(model.convert_row(*row))
            yield row

    def add_data(self):
        pass

    def save_schema(self, path):
        pass

    def save_data(self, path):
        pass

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

    def upload(self, schema, data, **options):
        """Upload schema+data to BigQuery.

        Parameters
        ----------
        schema (str/dict):
            Schema or path to schema (json) file to be uploaded.
        data (str):
            Path to data (csv) file to be uploaded.
        options (dict):
            Tabulator options.

        """

        # Read schema
        if not isinstance(schema, dict):
            with io.open(schema, encoding='utf-8') as file:
                schema = json.load(file)

        # Read data
        bytes = io.BufferedRandom(io.BytesIO())
        class Stream: #noqa
            def write(self, string):
                bytes.write(string.encode('utf-8'))
        stream = Stream()
        with topen(data, **options) as table:
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
