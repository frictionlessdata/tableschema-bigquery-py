# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import six
import time
import datetime
import unicodecsv
import jsontableschema
from jsontableschema import Schema
from apiclient.http import MediaIoBaseUpload
from . import mappers


# Module API

class Storage(object):
    """BigQuery Tabular Storage.

    It's an implementation of `jsontablescema.Storage`.

    Args:
        service (object): service object from API
        project (str): project name
        dataset (str): dataset name
        prefix (str): prefix for all buckets

    """

    # Public

    def __init__(self, service, project, dataset, prefix=''):

        # Set attributes
        self.__service = service
        self.__project = project
        self.__dataset = dataset
        self.__prefix = prefix
        self.__buckets = None
        self.__descriptors = {}

    def __repr__(self):

        # Template and format
        template = 'Storage <{service}/{project}-{dataset}>'
        text = template.format(
            service=self.__service,
            project=self.__project,
            dataset=self.__dataset)

        return text

    @property
    def buckets(self):

        # No cached value
        if self.__buckets is None:

            # Get response
            response = self.__service.tables().list(
                projectId=self.__project,
                datasetId=self.__dataset).execute()

            # Extract buckets
            self.__buckets = []
            for table in response.get('tables', []):
                tablename = table['tableReference']['tableId']
                bucket = mappers.tablename_to_bucket(self.__prefix, tablename)
                if bucket is not None:
                    self.__buckets.append(bucket)

        return self.__buckets

    def create(self, bucket, descriptor, force=False):

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
                    raise RuntimeError(message)
                self.delete(bucket)

            # Add to schemas
            self.__descriptors[bucket] = descriptor

            # Prepare job body
            jsontableschema.validate(descriptor)
            tablename = mappers.bucket_to_tablename(self.__prefix, bucket)
            nativedesc = mappers.descriptor_to_nativedesc(descriptor)
            body = {
                'tableReference': {
                    'projectId': self.__project,
                    'datasetId': self.__dataset,
                    'tableId': tablename,
                },
                'schema': nativedesc,
            }

            # Make request
            self.__service.tables().insert(
                projectId=self.__project,
                datasetId=self.__dataset,
                body=body).execute()

        # Remove buckets cache
        self.__buckets = None

    def delete(self, bucket=None, ignore=False):

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
                    raise RuntimeError(message)

            # Remove from descriptors
            if bucket in self.__descriptors:
                del self.__descriptors[bucket]

            # Make delete request
            tablename = mappers.bucket_to_tablename(self.__prefix, bucket)
            self.__service.tables().delete(
                projectId=self.__project,
                datasetId=self.__dataset,
                tableId=tablename).execute()

        # Remove tables cache
        self.__buckets = None

    def describe(self, bucket, descriptor=None):

        # Set descriptor
        if descriptor is not None:
            self.__descriptors[bucket] = descriptor

        # Get descriptor
        else:
            descriptor = self.__descriptors.get(bucket)
            if descriptor is None:
                tablename = mappers.bucket_to_tablename(self.__prefix, bucket)
                response = self.__service.tables().get(
                    projectId=self.__project,
                    datasetId=self.__dataset,
                    tableId=tablename).execute()
                nativedesc = response['schema']
                descriptor = mappers.nativedesc_to_descriptor(nativedesc)

        return descriptor

    def iter(self, bucket):

        # Get response
        descriptor = self.describe(bucket)
        schema = Schema(descriptor)
        tablename = mappers.bucket_to_tablename(self.__prefix, bucket)
        response = self.__service.tabledata().list(
            projectId=self.__project,
            datasetId=self.__dataset,
            tableId=tablename).execute()

        # Yield rows
        for fields in response['rows']:
            row = []
            values = [field['v'] for field in fields['f']]
            for index, field in enumerate(schema.fields):
                value = values[index]
                # Here we fix bigquery "1.234234E9" like datetimes
                if field.type == 'date':
                    value = datetime.datetime.utcfromtimestamp(
                        int(float(value)))
                    fmt = '%Y-%m-%d'
                    if field.format.startswith('fmt:'):
                        fmt = field.format.replace('fmt:', '')
                    value = value.strftime(fmt)
                elif field.type == 'datetime':
                    value = datetime.datetime.utcfromtimestamp(
                        int(float(value)))
                    value = '%sZ' % value.isoformat()
                row.append(value)
            yield schema.cast_row(row)

    def read(self, bucket):

        # Get rows
        rows = list(self.iter(bucket))

        return rows

    def write(self, bucket, rows):

        # Prepare
        BUFFER_SIZE = 10000

        # Write
        rows_buffer = []
        for row in rows:
            rows_buffer.append(row)
            if len(rows_buffer) > BUFFER_SIZE:
                self.__write_rows_buffer(bucket, rows_buffer)
                rows_buffer = []
        if len(rows_buffer) > 0:
            self.__write_rows_buffer(bucket, rows_buffer)

    # Private

    def __write_rows_buffer(self, bucket, rows_buffer):

        # Process data to byte stream csv
        descriptor = self.describe(bucket)
        schema = Schema(descriptor)
        bytes = io.BufferedRandom(io.BytesIO())
        writer = unicodecsv.writer(bytes, encoding='utf-8')
        for values in rows_buffer:
            row = []
            values = schema.cast_row(values)
            for index, field in enumerate(schema.fields):
                value = values[index]
                # Here we convert date to datetime
                if field.type == 'date':
                    value = datetime.datetime.fromordinal(value.toordinal())
                    value = '%sZ' % value.isoformat()
                row.append(value)
            writer.writerow(row)
        bytes.seek(0)

        # Prepare job body
        tablename = mappers.bucket_to_tablename(self.__prefix, bucket)
        body = {
            'configuration': {
                'load': {
                    'destinationTable': {
                        'projectId': self.__project,
                        'datasetId': self.__dataset,
                        'tableId': tablename
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
