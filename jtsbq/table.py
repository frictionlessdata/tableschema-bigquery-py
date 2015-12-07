# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import csv
import json
from jsontableschema.model import SchemaModel
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials


class Table(object):
    """BigQuery table representation.
    """

    # Public

    SCOPE = ['https://www.googleapis.com/auth/bigquery']
    TYPES = {
        'STRING': 'string',
        'INTEGER': 'integer',
        'FLOAT': 'number',
        'BOOLEAN': 'boolean',
        'TIMESTAMP': 'datetime',
    }

    def __init__(self, client_email, private_key,
                 project_id, dataset_id, table_id):
        self.__client_email = client_email
        self.__private_key = private_key
        self.__project_id = project_id
        self.__dataset_id = dataset_id
        self.__table_id = table_id

    def download(self, schema_path, data_path):
        """Download table schema and data

        Directory of the files has to be existent.

        Parameters
        ----------
        schema_path (str):
            Path to schema (json) file to be saved.
        data_path (str):
            Path to data (csv) file to be saved.

        """

        # Prepare headers and rows
        schema = self.__schema
        model = SchemaModel(schema)
        headers = model.headers
        rows = []
        for row in self.__rows:
            row = tuple(model.convert_row(*row))
            rows.append(row)

        # Write files on disk
        with open(schema_path, 'w') as file:
            json.dump(schema, file, indent=4)
        with open(data_path, 'w') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            # TODO: remove additional loop
            for row in rows:
                writer.writerow(row)

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
        schema = table['schema']

        # Convert schema
        fields = []
        for field in schema['fields']:
            try:
                ftype = self.TYPES[field['type']]
            except KeyError:
                message = 'Type %s is not supported' % field['type']
                raise TypeError(message)
            # TODO: fix required
            fields.append({
                'name': field['name'],
                'type': ftype,
            })
        self.___schema = {'fields': fields}

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
