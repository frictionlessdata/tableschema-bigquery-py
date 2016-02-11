# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import six
import time
import jsontableschema
import unicodecsv as csv
from jsontableschema.model import SchemaModel
from apiclient.http import MediaIoBaseUpload

from . import helpers


# Module API

class Storage(object):
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
                table = helpers.restore_table(table, self.__prefix)
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

            # Validate schema
            jsontableschema.validate(schema)

            # Prepare job body
            schema = helpers.convert_schema(schema)
            table = helpers.convert_table(table, self.__prefix)
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

            # Make delete request
            table = helpers.convert_table(table, self.__prefix)
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

        # Get response
        table = helpers.convert_table(table, self.__prefix)
        response = self.__service.tables().get(
                projectId=self.__project,
                datasetId=self.__dataset,
                tableId=table).execute()

        # Get schema
        schema = response['schema']
        schema = helpers.restore_schema(schema)

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
        table = helpers.convert_table(table, self.__prefix)
        response = self.__service.tabledata().list(
                projectId=self.__project,
                datasetId=self.__dataset,
                tableId=table).execute()

        # Yield data
        for row in response['rows']:
            row = tuple(field['v'] for field in row['f'])
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

        # Process data to byte stream csv
        schema = self.describe(table)
        model = SchemaModel(schema)
        bytes = io.BufferedRandom(io.BytesIO())
        writer = csv.writer(bytes, encoding='utf-8')
        for row in data:
            row = tuple(model.convert_row(*row, fail_fast=True))
            writer.writerow(row)
        bytes.seek(0)

        # Prepare job body
        table = helpers.convert_table(table, self.__prefix)
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

    # Private

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
