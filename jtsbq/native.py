# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import csv
import time
from apiclient.discovery import build
from apiclient.http import MediaIoBaseUpload


class NativeTable(object):
    """BigQuery native table representation.
    """

    # Public

    def __init__(self, project_id, dataset_id, table_id,
                 credentials, schema=None):

        # Set attributes
        self.__project_id = project_id
        self.__dataset_id = dataset_id
        self.__table_id = table_id

        # Initiate service
        self.__service = build('bigquery', 'v2', credentials=credentials)

        # Set schema
        try:
            self.__schema = self.get_schema()
        except Exception:
            # TODO: filter exceptions
            if schema is None:
                message = 'Non existent table requres schema argument'
                raise RuntimeError(message)
            self.__create_table(schema)
            self.__schema = schema

    def get_schema(self):
        """Return schema dict.
        """

        # Return from cache
        if hasattr(self, '__schema'):
            return self.__schema

        # Get response
        response = self.__service.tables().get(
                projectId=self.__project_id,
                datasetId=self.__dataset_id,
                tableId=self.__table_id).execute()

        return response['schema']

    def get_data(self):
        """Return table's data.
        """

        # Get respose
        template = 'SELECT * FROM [{project_id}:{dataset_id}.{table_id}];'
        query = template.format(
                project_id=self.__project_id,
                dataset_id=self.__dataset_id,
                table_id=self.__table_id)
        response = self.__service.jobs().query(
            projectId=self.__project_id,
            body={'query': query}).execute()

        # Yield rows
        for row in response['rows']:
            yield tuple(field['v'] for field in row['f'])

    def add_data(self, data):
        """Add data to table.
        """

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
        body = {
            'configuration': {
                'load': {
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

        # Make request to Big Query
        response = self.__service.jobs().insert(
            projectId=self.__project_id,
            body=body,
            media_body=media_body).execute()
        self.__wait_response(response)

    # Private

    def __create_table(self, schema):

        # Prepare job body
        body = {
            'tableReference': {
                'projectId': self.__project_id,
                'datasetId': self.__dataset_id,
                'tableId': self.__table_id,
            },
            'schema': schema,
        }

        # Make request
        self.__service.tables().get(
                projectId=self.__project_id,
                datasetId=self.__dataset_id,
                body=body).execute()

    def __wait_response(self, response):

        # Get job instance
        job = self.__service.jobs().get(
            projectId=self.__project_id,
            jobId=response['jobReference']['jobId'])

        # Wait done
        while True:
            result = job.execute(num_retries=2)
            if result['status']['state'] == 'DONE':
                if result['status'].get('errors'):
                    errors = result['status']['errors']
                    message = '\n'.join(error['message'] for error in errors)
                    raise RuntimeError(message)
                break
            time.sleep(1)
