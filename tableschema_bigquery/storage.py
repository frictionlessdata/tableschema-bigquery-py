# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import six
import time
import unicodecsv
import tableschema
from apiclient.http import MediaIoBaseUpload
from .mapper import Mapper


# Module API

class Storage(tableschema.Storage):

    # Public

    def __init__(self, service, project, dataset, prefix=''):
        """https://github.com/frictionlessdata/tableschema-bigquery-py#storage
        """

        # Set attributes
        self.__service = service
        self.__project = project
        self.__dataset = dataset
        self.__prefix = prefix
        self.__buckets = None
        self.__descriptors = {}
        self.__fallbacks = {}

        # Create mapper
        self.__mapper = Mapper(prefix=prefix)

    def __repr__(self):
        """https://github.com/frictionlessdata/tableschema-bigquery-py#storage
        """

        # Template and format
        template = 'Storage <{service}/{project}-{dataset}>'
        text = template.format(
            service=self.__service,
            project=self.__project,
            dataset=self.__dataset)

        return text

    @property
    def buckets(self):
        """https://github.com/frictionlessdata/tableschema-bigquery-py#storage
        """

        # No cached value
        if self.__buckets is None:

            # Get response
            response = self.__service.tables().list(
                projectId=self.__project,
                datasetId=self.__dataset).execute()

            # Extract buckets
            self.__buckets = []
            for table in response.get('tables', []):
                table_name = table['tableReference']['tableId']
                bucket = self.__mapper.restore_bucket(table_name)
                if bucket is not None:
                    self.__buckets.append(bucket)

        return self.__buckets

    def create(self, bucket, descriptor, force=False):
        """https://github.com/frictionlessdata/tableschema-bigquery-py#storage
        """

        # Make lists
        buckets = bucket
        if isinstance(bucket, six.string_types):
            buckets = [bucket]
        descriptors = descriptor
        if isinstance(descriptor, dict):
            descriptors = [descriptor]

        # Iterate over buckets/descriptors
        for bucket, descriptor in zip(buckets, descriptors):

            # Existent bucket
            if bucket in self.buckets:
                if not force:
                    message = 'Bucket "%s" already exists' % bucket
                    raise tableschema.exceptions.StorageError(message)
                self.delete(bucket)

            # Prepare job body
            tableschema.validate(descriptor)
            table_name = self.__mapper.convert_bucket(bucket)
            converted_descriptor, fallbacks = self.__mapper.convert_descriptor(descriptor)
            body = {
                'tableReference': {
                    'projectId': self.__project,
                    'datasetId': self.__dataset,
                    'tableId': table_name,
                },
                'schema': converted_descriptor,
            }

            # Make request
            self.__service.tables().insert(
                projectId=self.__project,
                datasetId=self.__dataset,
                body=body).execute()

            # Add to descriptors/fallbacks
            self.__descriptors[bucket] = descriptor
            self.__fallbacks[bucket] = fallbacks

        # Remove buckets cache
        self.__buckets = None

    def delete(self, bucket=None, ignore=False):
        """https://github.com/frictionlessdata/tableschema-bigquery-py#storage
        """

        # Make lists
        buckets = bucket
        if isinstance(bucket, six.string_types):
            buckets = [bucket]
        elif bucket is None:
            buckets = reversed(self.buckets)

        # Iterater over buckets
        for bucket in buckets:

            # Non-existent bucket
            if bucket not in self.buckets:
                if not ignore:
                    message = 'Bucket "%s" doesn\'t exist.' % bucket
                    raise tableschema.exceptions.StorageError(message)
                return

            # Remove from descriptors
            if bucket in self.__descriptors:
                del self.__descriptors[bucket]

            # Make delete request
            table_name = self.__mapper.convert_bucket(bucket)
            self.__service.tables().delete(
                projectId=self.__project,
                datasetId=self.__dataset,
                tableId=table_name).execute()

        # Remove tables cache
        self.__buckets = None

    def describe(self, bucket, descriptor=None):
        """https://github.com/frictionlessdata/tableschema-bigquery-py#storage
        """

        # Set descriptor
        if descriptor is not None:
            self.__descriptors[bucket] = descriptor

        # Get descriptor
        else:
            descriptor = self.__descriptors.get(bucket)
            if descriptor is None:
                table_name = self.__mapper.convert_bucket(bucket)
                response = self.__service.tables().get(
                    projectId=self.__project,
                    datasetId=self.__dataset,
                    tableId=table_name).execute()
                converted_descriptor = response['schema']
                descriptor = self.__mapper.restore_descriptor(converted_descriptor)

        return descriptor

    def iter(self, bucket):
        """https://github.com/frictionlessdata/tableschema-bigquery-py#storage
        """

        # Get schema/data
        schema = tableschema.Schema(self.describe(bucket))
        table_name = self.__mapper.convert_bucket(bucket)
        response = self.__service.tabledata().list(
            projectId=self.__project,
            datasetId=self.__dataset,
            tableId=table_name).execute()

        # Collect rows
        rows = []
        for fields in response['rows']:
            row = [field['v'] for field in fields['f']]
            rows.append(row)

        # Sort rows
        # TODO: provide proper sorting solution
        rows = sorted(rows, key=lambda row: row[0] if row[0] is not None else 'null')

        # Emit rows
        for row in rows:
            row = self.__mapper.restore_row(row, schema=schema)
            yield row

    def read(self, bucket):
        """https://github.com/frictionlessdata/tableschema-bigquery-py#storage
        """
        rows = list(self.iter(bucket))
        return rows

    def write(self, bucket, rows):
        """https://github.com/frictionlessdata/tableschema-bigquery-py#storage
        """

        # Write buffer
        BUFFER_SIZE = 10000

        # Prepare schema, fallbacks
        schema = tableschema.Schema(self.describe(bucket))
        fallbacks = self.__fallbacks.get(bucket, [])

        # Write data
        rows_buffer = []
        for row in rows:
            row = self.__mapper.convert_row(row, schema=schema, fallbacks=fallbacks)
            rows_buffer.append(row)
            if len(rows_buffer) > BUFFER_SIZE:
                self.__write_rows_buffer(bucket, rows_buffer)
                rows_buffer = []
        if len(rows_buffer) > 0:
            self.__write_rows_buffer(bucket, rows_buffer)

    # Private

    def __write_rows_buffer(self, bucket, rows_buffer):

        # Process data to byte stream csv
        bytes = io.BufferedRandom(io.BytesIO())
        writer = unicodecsv.writer(bytes, encoding='utf-8')
        for row in rows_buffer:
            writer.writerow(row)
        bytes.seek(0)

        # Prepare job body
        table_name = self.__mapper.convert_bucket(bucket)
        body = {
            'configuration': {
                'load': {
                    'destinationTable': {
                        'projectId': self.__project,
                        'datasetId': self.__dataset,
                        'tableId': table_name
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
                    raise tableschema.exceptions.StorageError(message)
                break
            time.sleep(1)
