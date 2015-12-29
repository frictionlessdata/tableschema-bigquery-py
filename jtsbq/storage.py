# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import csv
import time
from apiclient.http import MediaIoBaseUpload


# Module API

class Storage(object):

    # Public

    def __init__(self, service, project, dataset, prefix=''):

        # Set attributes
        self.__service = service
        self.__project = project
        self.__dataset = dataset
        self.__prefix = prefix
        self.__tables = None

    def __repr__(self):

        # Template and format
        template = 'Storage <{service}/{project}-{dataset}>'
        text = template.format(
                service=self.__service,
                project=self.__project,
                dataset=self.__dataset)

        return text

    @property
    def tables(self):
        """List of table names.
        """

        # No cached value
        if self.__tables is None:

            # Get response
            response = self.__service.tables().list(
                    projectId=self.__project,
                    datasetId=self.__dataset).execute()

            # Extract tables
            tables = []
            for table in response.get('tables', []):
                table = table['tableReference']['tableId']
                if table.startswith(self.__prefix):
                    table = table.replace(self.__prefix, '', 1)
                    tables.append(table)

            # Save to cache
            self.__tables = tables

        return self.__tables

    def check(self, table):
        """Return true if table exists.
        """
        return table in self.tables

    def create(self, table, schema):
        """Create table by schema.

        Parameters
        ----------
        table: str
            Table name.
        schema: dict
            BigQuery schema descriptor.

        Raises
        ------
        RuntimeError
            If table is already existent.

        """

        # Check not existent
        if self.check(table):
            message = 'Table "%s" is already existent.' % table
            raise RuntimeError(message)

        # Convert jts schema
        schema = self.__convert_schema(schema)

        # Prepare job body
        name = self.__prefix + table
        body = {
            'tableReference': {
                'projectId': self.__project,
                'datasetId': self.__dataset,
                'tableId': name,
            },
            'schema': schema,
        }

        # Make request
        self.__service.tables().insert(
                projectId=self.__project,
                datasetId=self.__dataset,
                body=body).execute()

        # Remove tables cache
        self.__tables = None

    def delete(self, table):
        """Delete table.

        Raises
        ------
        RuntimeError
            If table is not existent.

        """

        # Check existent
        if not self.check(table):
            message = 'Table "%s" is not existent.' % self
            raise RuntimeError(message)

        # Make request
        name = self.__prefix + table
        self.__service.tables().delete(
                projectId=self.__project,
                datasetId=self.__dataset,
                tableId=name).execute()

        # Remove tables cache
        self.__tables = None

    def describe(self, table):

        # Get response
        name = self.__prefix + table
        response = self.__service.tables().get(
                projectId=self.__project,
                datasetId=self.__dataset,
                tableId=name).execute()

        # Get schema
        schema = response['schema']
        schema = self.__restore_schema(schema)

        return schema

    def read(self, table):

        # Get response
        name = self.__prefix + table
        response = self.__service.tabledata().list(
                projectId=self.__project,
                datasetId=self.__dataset,
                tableId=name).execute()

        # Yield rows
        for row in response['rows']:
            yield tuple(field['v'] for field in row['f'])

    def write(self, table, data):

        # Convert data to byte stream csv
        bytes = io.BufferedRandom(io.BytesIO())
        class Stream: #noqa
            def write(self, string):
                bytes.write(string.encode('utf-8'))
        stream = Stream()
        writer = csv.writer(stream)
        for row in data:
            writer.writerow(row)
        bytes.seek(0)

        # Prepare job body
        name = self.__prefix + table
        body = {
            'configuration': {
                'load': {
                    'destinationTable': {
                        'projectId': self.__project,
                        'datasetId': self.__dataset,
                        'tableId': name
                    },
                    'sourceFormat': 'CSV',
                }
            }
        }

        # Prepare job media body
        mimetype = 'application/octet-stream'
        media_body = MediaIoBaseUpload(bytes, mimetype=mimetype)

        # Make request to Big Query
        response = self.__service.jobs().insert(
            projectId=self.__project,
            body=body,
            media_body=media_body).execute()
        self.__wait_response(response)

    # Private

    def __convert_schema(self, schema):
        """Convert JSONTableSchema schema to SQLAlchemy columns.
        """

        # Mapping
        mapping = {
            'string': 'STRING',
            'integer': 'INTEGER',
            'number': 'FLOAT',
            'boolean': 'BOOLEAN',
            'datetime': 'TIMESTAMP',
        }

        fields = []
        for field in schema['fields']:
            try:
                ftype = mapping[field['type']]
            except KeyError:
                message = 'Type %s is not supported' % field['type']
                raise TypeError(message)
            fields.append({
                'name': field['name'],
                'type': ftype,
            })
        schema = {'fields': fields}

        return schema

    def __restore_schema(self, schema):
        """Convert SQLAlchemy table reflection to JSONTableSchema schema.
        """

        # Mapping
        mapping = {
            'STRING': 'string',
            'INTEGER': 'integer',
            'FLOAT': 'number',
            'BOOLEAN': 'boolean',
            'TIMESTAMP': 'datetime',
        }

        fields = []
        for field in schema['fields']:
            try:
                ftype = mapping[field['type']]
            except KeyError:
                message = 'Type %s is not supported' % field['type']
                raise TypeError(message)
            fields.append({
                'name': field['name'],
                'type': ftype,
            })
        schema = {'fields': fields}

        return schema

    def __wait_response(self, response):

        # Get job instance
        job = self.__service.jobs().get(
            projectId=response['jobReference']['projectId'],
            jobId=response['jobReference']['jobId'])

        # Wait done
        while True:
            result = job.execute(num_retries=1)
            if result['status']['state'] == 'DONE':
                if result['status'].get('errors'):
                    errors = result['status']['errors']
                    message = '\n'.join(error['message'] for error in errors)
                    raise RuntimeError(message)
                break
            time.sleep(1)
