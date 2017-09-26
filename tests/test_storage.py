# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import io
import json
import uuid
import pytest
import datetime
from copy import deepcopy
from decimal import Decimal
from tabulator import Stream
from jsontableschema import Schema
from apiclient.discovery import build
from oauth2client.client import GoogleCredentials
from tableschema_bigquery import Storage


# Tests

def test_storage():

    # Get resources
    articles_descriptor = json.load(io.open('data/articles.json', encoding='utf-8'))
    articles_rows = Stream('data/articles.csv', headers=1).open().read()

    # Prepare BigQuery
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '.credentials.json'
    credentials = GoogleCredentials.get_application_default()
    service = build('bigquery', 'v2', credentials=credentials)
    project = json.load(io.open('.credentials.json', encoding='utf-8'))['project_id']
    dataset = 'resource'
    prefix = '%s_' % uuid.uuid4().hex

    # Storage
    storage = Storage(service, project, dataset, prefix=prefix)

    # Delete buckets
    storage.delete()

    # Create buckets
    storage.create('articles', articles_descriptor)

    # Write data to buckets
    storage.write('articles', articles_rows)

    # Create new storage to use reflection only
    storage = Storage(service, project, dataset, prefix=prefix)

    # Create existent bucket
    with pytest.raises(RuntimeError):
        storage.create('articles', articles_descriptor)

    # Assert representation
    assert repr(storage).startswith('Storage')

    # Assert buckets
    assert storage.buckets == ['articles']

    # Assert descriptors
    assert storage.describe('articles') == sync_descriptor(articles_descriptor)

    # Assert rows
    assert list(storage.read('articles')) == sunc_rows(articles_descriptor, articles_rows)

    # Delete non existent bucket
    with pytest.raises(RuntimeError):
        storage.delete('non_existent')

    # Delete buckets
    storage.delete()


def test_storage_bigdata():

    # Generate schema/data
    descriptor = {'fields': [{'name': 'id', 'type': 'integer'}]}
    rows = [[value,] for value in range(0, 15000)]

    # Push rows
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '.credentials.json'
    credentials = GoogleCredentials.get_application_default()
    service = build('bigquery', 'v2', credentials=credentials)
    project = json.load(io.open('.credentials.json', encoding='utf-8'))['project_id']
    dataset = 'resource'
    prefix = '%s_' % uuid.uuid4().hex
    storage = Storage(service, project, dataset, prefix=prefix)
    storage.create('bucket', descriptor, force=True)
    storage.write('bucket', rows)

    # Pull rows
    assert list(storage.read('bucket')) == rows


# Helpers

def sync_descriptor(descriptor):
    descriptor = deepcopy(descriptor)
    for field in descriptor['fields']:
        if field['type'] in ['date']:
            field['type'] = 'datetime'
        elif field['type'] in ['array', 'geojson']:
            field['type'] = 'object'
        if 'format' in field:
            del field['format']
    return descriptor

def sunc_rows(descriptor, rows):
    result = []
    schema = Schema(descriptor)
    for row in rows:
        row = schema.cast_row(row)
        values = []
        for index, field in enumerate(descriptor['fields']):
            value = row[index]
            if field['type'] == 'date':
                value = datetime.datetime.fromordinal(value.toordinal())
            values.append(value)
        result.append(values)
    return result
