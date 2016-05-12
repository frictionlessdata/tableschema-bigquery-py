# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import six
import time
import datetime
import jsontableschema
import unicodecsv as csv
from jsontableschema import storage as base
from jsontableschema.model import SchemaModel
from apiclient.http import MediaIoBaseUpload

from . import mappers


# Module API

class Storage(base.Storage):
    """BigQuery Tabular Storage.

    Parameters
    ----------
    service: object
        Service object from API.
    project: str
        Project name.
    dataset: str
        Dataset name.
    prefix: str
        Prefix for all tables.

    """

    # Public

    def __init__(self, service, project, dataset, prefix=''):

        # Set attributes
        self.__service = service
        self.__project = project
        self.__dataset = dataset
        self.__prefix = prefix
        self.__tables = None
        self.__schemas = {}

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
        """Return list of storage's table names.
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
                table = mappers.restore_table(self.__prefix, table)
                if table is not None:
                    tables.append(table)

            # Save to cache
            self.__tables = tables

        return self.__tables

    def check(self, table):
        """Return if table exists.
        """

        # Check existence
        existence = table in self.tables

        return existence

    def create(self, table, schema):
        """Create table by schema.

        Parameters
        ----------
        table: str/list
            Table name or list of table names.
        schema: dict/list
            JSONTableSchema schema or list of schemas.

        Raises
        ------
        RuntimeError
            If table already exists.

        """

        # Make lists
        tables = table
        if isinstance(table, six.string_types):
            tables = [table]
        schemas = schema
        if isinstance(schema, dict):
            schemas = [schema]

        # Iterate over tables/schemas
        for table, schema in zip(tables, schemas):

            # Check not existent
            if self.check(table):
                message = 'Table "%s" already exists' % table
                raise RuntimeError(message)

            # Add to schemas
            self.__schemas[table] = schema

            # Prepare job body
            table = mappers.convert_table(self.__prefix, table)
            jsontableschema.validate(schema)
            schema = mappers.convert_schema(schema)
            body = {
                'tableReference': {
                    'projectId': self.__project,
                    'datasetId': self.__dataset,
                    'tableId': table,
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

        Parameters
        ----------
        table: str/list
            Table name or list of table names.

        Raises
        ------
        RuntimeError
            If table doesn't exist.

        """

        # Make lists
        tables = table
        if isinstance(table, six.string_types):
            tables = [table]

        # Iterater over tables
        for table in tables:

            # Check existent
            if not self.check(table):
                message = 'Table "%s" doesn\'t exist.' % self
                raise RuntimeError(message)

            # Remove from schemas
            if table in self.__schemas:
                del self.__schemas[table]

            # Make delete request
            table = mappers.convert_table(self.__prefix, table)
            self.__service.tables().delete(
                    projectId=self.__project,
                    datasetId=self.__dataset,
                    tableId=table).execute()

        # Remove tables cache
        self.__tables = None

    def describe(self, table):
        """Return table's JSONTableSchema schema.

        Parameters
        ----------
        table: str
            Table name.

        Returns
        -------
        dict
            JSONTableSchema schema.

        """

        # Get schema
        if table in self.__schemas:
            schema = self.__schemas[table]
        else:
            table = mappers.convert_table(self.__prefix, table)
            response = self.__service.tables().get(
                    projectId=self.__project,
                    datasetId=self.__dataset,
                    tableId=table).execute()
            schema = response['schema']
            schema = mappers.restore_schema(schema)

        return schema

    def read(self, table):
        """Read data from table.

        Parameters
        ----------
        table: str
            Table name.

        Returns
        -------
        generator
            Data tuples generator.

        """

        # Get response
        schema = self.describe(table)
        model = SchemaModel(schema)
        table = mappers.convert_table(self.__prefix, table)
        response = self.__service.tabledata().list(
                projectId=self.__project,
                datasetId=self.__dataset,
                tableId=table).execute()

        # Yield data
        for row in response['rows']:
            values = tuple(field['v'] for field in row['f'])
            row = []

            # TODO: move to mappers.restore_row?
            for index, field in enumerate(model.fields):
                value = values[index]
                # Here we fix bigquery "1.234234E9" like datetimes
                if field['type'] == 'date':
                    value = datetime.datetime.utcfromtimestamp(int(float(value)))
                    # TODO: remove after jsontableschema-py/59 fix
                    fmt = '%Y-%m-%d'
                    if field.get('format', '').startswith('fmt:'):
                        fmt = field['format'].replace('fmt:', '')
                    value = value.strftime(fmt)
                elif field['type'] == 'datetime':
                    value = datetime.datetime.utcfromtimestamp(int(float(value)))
                    # TODO: remove after jsontableschema-py/59 fix
                    value = '%sZ' % value.isoformat()
                row.append(value)

            row = tuple(model.convert_row(*row, fail_fast=True))
            yield row

    def write(self, table, data):
        """Write data to table.

        Parameters
        ----------
        table: str
            Table name.
        data: list
            List of data tuples.

        """
        BUFFER_ROWS = 10000

        data_chunk = []
        for row in data:
            data_chunk.append(row)
            if len(data_chunk) > BUFFER_ROWS:
                self.__write_data_chunk(table, data_chunk)
                data_chunk = []
        if len(data_chunk) > 0:
            self.__write_data_chunk(table, data_chunk)

    # Private

    def __write_data_chunk(self, table, data_chunk):

        # Process data to byte stream csv
        schema = self.describe(table)
        model = SchemaModel(schema)
        bytes = io.BufferedRandom(io.BytesIO())
        writer = csv.writer(bytes, encoding='utf-8')
        for values in data_chunk:
            row = []
            values = tuple(model.convert_row(*values, fail_fast=True))
            # TODO: move to mappers.convert_row?
            for index, field in enumerate(model.fields):
                value = values[index]
                # Here we convert date to datetime
                if field['type'] == 'date':
                    value = datetime.datetime.fromordinal(value.toordinal())
                    # TODO: remove after jsontableschema-py/59 fix
                    value = '%sZ' % value.isoformat()
                row.append(value)
            writer.writerow(row)
        bytes.seek(0)

        # Prepare job body
        table = mappers.convert_table(self.__prefix, table)
        body = {
            'configuration': {
                'load': {
                    'destinationTable': {
                        'projectId': self.__project,
                        'datasetId': self.__dataset,
                        'tableId': table
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
