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

class Table(object):
    """BigQuery native table representation.
    """

    # Public

    def __init__(self, service, project_id, dataset_id, table_id,
                 schema=None):

        # Set attributes
        self.__service = service
        self.__project_id = project_id
        self.__dataset_id = dataset_id
        self.__table_id = table_id

        # Ensure existent table
        try:
            self.schema
        except Exception:
            # TODO: filter exceptions
            if schema is None:
                message = 'Non existent table requires schema argument'
                raise RuntimeError(message)
            self.__create_table(schema)

    @property
    def schema(self):
        """Return schema dict.
        """

        # Create cache
        if not hasattr(self, '__schema'):

            # Get response
            response = self.__service.tables().get(
                    projectId=self.__project_id,
                    datasetId=self.__dataset_id,
                    tableId=self.__table_id).execute()

            # Get schema
            self.__schema = response['schema']

        return self.__schema

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
