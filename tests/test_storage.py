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
from tabulator import topen
from apiclient.discovery import build
from oauth2client.client import GoogleCredentials
from jsontableschema.model import SchemaModel

from jsontableschema_bigquery import Storage


# Tests

def test_storage():

    # Get resources
    articles_schema = json.load(io.open('data/articles.json', encoding='utf-8'))
    articles_data = topen('data/articles.csv', with_headers=True).read()

    # Prepare BigQuery
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '.credentials.json'
    credentials = GoogleCredentials.get_application_default()
    service = build('bigquery', 'v2', credentials=credentials)
    project = json.load(io.open('.credentials.json', encoding='utf-8'))['project_id']
    dataset = 'resource'
    prefix = '%s_' % uuid.uuid4().hex

    # Storage
    storage = Storage(service, project, dataset, prefix=prefix)

    # Delete tables
    for table in reversed(storage.tables):
        storage.delete(table)

    # Create tables
    storage.create('articles', articles_schema)

    # Write data to tables
    storage.write('articles', articles_data)

    # Create new storage to use reflection only
    storage = Storage(service, project, dataset, prefix=prefix)

    # Create existent table
    with pytest.raises(RuntimeError):
        storage.create('articles', articles_schema)

    # Get table representation
    assert repr(storage).startswith('Storage')

    # Get tables list
    assert storage.tables == ['articles']

    # Get table schemas
    assert storage.describe('articles') == convert_schema(articles_schema)

    # Get table data
    assert list(storage.read('articles')) == convert_data(articles_schema, articles_data)

    # Delete tables
    for table in reversed(storage.tables):
        storage.delete(table)

    # Delete non existent table
    with pytest.raises(RuntimeError):
        storage.delete('articles')


def test_storage_bigdata():

    # Generate schema/data
    schema = {'fields': [{'name': 'id', 'type': 'integer'}]}
    data = [(value,) for value in range(0, 15000)]

    # Push data
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '.credentials.json'
    credentials = GoogleCredentials.get_application_default()
    service = build('bigquery', 'v2', credentials=credentials)
    project = json.load(io.open('.credentials.json', encoding='utf-8'))['project_id']
    dataset = 'resource'
    prefix = '%s_' % uuid.uuid4().hex
    storage = Storage(service, project, dataset, prefix=prefix)
    for table in reversed(storage.tables):
        storage.delete(table)
    storage.create('table', schema)
    storage.write('table', data)

    # Pull data
    assert list(storage.read('table')) == data


# Helpers

def convert_schema(schema):
    schema = deepcopy(schema)
    for field in schema['fields']:
        if field['type'] in ['date']:
            field['type'] = 'datetime'
        elif field['type'] in ['array', 'geojson']:
            field['type'] = 'object'
        if 'format' in field:
            del field['format']
    return schema

def convert_data(schema, data):
    result = []
    model = SchemaModel(schema)
    for item in data:
        item = tuple(model.convert_row(*item))
        values = []
        for index, field in enumerate(schema['fields']):
            value = item[index]
            if field['type'] == 'date':
                value = datetime.datetime.fromordinal(value.toordinal())
            values.append(value)
        result.append(tuple(values))
    return result
