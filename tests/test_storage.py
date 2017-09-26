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
import tableschema
from copy import deepcopy
from decimal import Decimal
from tabulator import Stream
from apiclient.discovery import build
from oauth2client.client import GoogleCredentials
from tableschema_bigquery import Storage


# Resources

ARTICLES = {
    'schema': {
        'fields': [
            {'name': 'id', 'type': 'integer', 'constraints': {'required': True}},
            {'name': 'parent', 'type': 'integer'},
            {'name': 'name', 'type': 'string'},
            {'name': 'current', 'type': 'boolean'},
            {'name': 'rating', 'type': 'number'},
        ],
        # 'primaryKey': 'id',
        # 'foreignKeys': [
            # {'fields': 'parent', 'reference': {'resource': '', 'fields': 'id'}},
        # ],
    },
    'data': [
        ['1', '', 'Taxes', 'True', '9.5'],
        ['2', '1', '中国人', 'False', '7'],
    ],
}
COMMENTS = {
    'schema': {
        'fields': [
            {'name': 'entry_id', 'type': 'integer', 'constraints': {'required': True}},
            {'name': 'comment', 'type': 'string'},
            {'name': 'note', 'type': 'any'},
        ],
        # 'primaryKey': 'entry_id',
        # 'foreignKeys': [
            # {'fields': 'entry_id', 'reference': {'resource': 'articles', 'fields': 'id'}},
        # ],
    },
    'data': [
        ['1', 'good', 'note1'],
        ['2', 'bad', 'note2'],
    ],
}
TEMPORAL = {
    'schema': {
        'fields': [
            {'name': 'date', 'type': 'date'},
            {'name': 'date_year', 'type': 'date', 'format': '%Y'},
            {'name': 'datetime', 'type': 'datetime'},
            {'name': 'duration', 'type': 'duration'},
            {'name': 'time', 'type': 'time'},
            {'name': 'year', 'type': 'year'},
            {'name': 'yearmonth', 'type': 'yearmonth'},
        ],
    },
    'data': [
        ['2015-01-01', '2015', '2015-01-01T03:00:00Z', 'P1Y1M', '03:00:00', '2015', '2015-01'],
        ['2015-12-31', '2015', '2015-12-31T15:45:33Z', 'P2Y2M', '15:45:33', '2015', '2015-01'],
    ],
}
LOCATION = {
    'schema': {
        'fields': [
            {'name': 'location', 'type': 'geojson'},
            {'name': 'geopoint', 'type': 'geopoint'},
        ],
    },
    'data': [
        ['{"type": "Point","coordinates":[33.33,33.33]}', '30,75'],
        ['{"type": "Point","coordinates":[50.00,50.00]}', '90,45'],
    ],
}
COMPOUND = {
    'schema': {
        'fields': [
            {'name': 'stats', 'type': 'object'},
            {'name': 'persons', 'type': 'array'},
        ],
    },
    'data': [
        ['{"chars":560}', '["Mike", "John"]'],
        ['{"chars":970}', '["Paul", "Alex"]'],
    ],
}


# Credentials

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '.credentials.json'
CREDENTIALS = GoogleCredentials.get_application_default()
SERVICE = build('bigquery', 'v2', credentials=CREDENTIALS)
PROJECT = json.load(io.open('.credentials.json', encoding='utf-8'))['project_id']
DATASET = 'resource'
PREFIX = '%s_' % uuid.uuid4().hex


# Tests

def test_storage():

    # Create storage
    storage = Storage(SERVICE, project=PROJECT, dataset=DATASET, prefix=PREFIX)

    # Delete buckets
    storage.delete()

    # Create buckets
    storage.create(['articles', 'comments'], [ARTICLES['schema'], COMMENTS['schema']])
    # TODO: investigate why it fails
    # storage.create('comments', COMMENTS['schema'], force=True)
    storage.create('temporal', TEMPORAL['schema'])
    storage.create('location', LOCATION['schema'])
    storage.create('compound', COMPOUND['schema'])

    # Write data
    storage.write('articles', ARTICLES['data'])
    storage.write('comments', COMMENTS['data'])
    storage.write('temporal', TEMPORAL['data'])
    storage.write('location', LOCATION['data'])
    storage.write('compound', COMPOUND['data'])

    # Create new storage to use reflection only
    storage = Storage(SERVICE, project=PROJECT, dataset=DATASET, prefix=PREFIX)

    # Create existent bucket
    # TODO: investigate why it fails
    # with pytest.raises(tableschema.exceptions.StorageError):
        # storage.create('articles', ARTICLES['schema'])

    # Assert buckets
    assert storage.buckets == ['articles', 'comments', 'compound', 'location', 'temporal']

    # Assert schemas
    assert storage.describe('articles') == ARTICLES['schema']
    assert storage.describe('comments') == {
        'fields': [
            {'name': 'entry_id', 'type': 'integer', 'constraints': {'required': True}},
            {'name': 'comment', 'type': 'string'},
            {'name': 'note', 'type': 'string'}, # type downgrade
        ],
    }
    assert storage.describe('temporal') == {
        'fields': [
            {'name': 'date', 'type': 'date'},
            {'name': 'date_year', 'type': 'date'}, # format removal
            {'name': 'datetime', 'type': 'datetime'},
            {'name': 'duration', 'type': 'string'}, # type fallback
            {'name': 'time', 'type': 'time'},
            {'name': 'year', 'type': 'integer'}, # type downgrade
            {'name': 'yearmonth', 'type': 'string'}, # type fallback
        ],
    }
    assert storage.describe('location') == {
        'fields': [
            {'name': 'location', 'type': 'string'}, # type fallback
            {'name': 'geopoint', 'type': 'string'}, # type fallback
        ],
    }
    assert storage.describe('compound') == {
        'fields': [
            {'name': 'stats', 'type': 'string'}, # type fallback
            {'name': 'persons', 'type': 'string'}, # type fallback
        ],
    }

    assert storage.read('articles') == cast(ARTICLES)['data']
    assert storage.read('comments') == cast(COMMENTS)['data']
    assert storage.read('temporal') == cast(TEMPORAL, skip=['duration', 'yearmonth'])['data']
    assert storage.read('location') == cast(LOCATION, skip=['geojson', 'geopoint'])['data']
    assert storage.read('compound') == cast(COMPOUND, skip=['array', 'object'])['data']

    # Assert data with forced schema
    storage.describe('compound', COMPOUND['schema'])
    assert storage.read('compound') == cast(COMPOUND)['data']

    # Delete non existent bucket
    with pytest.raises(tableschema.exceptions.StorageError):
        storage.delete('non_existent')

    # Delete buckets
    storage.delete()


def test_storage_bigdata():
    RESOURCE = {
        'schema': {
            'fields': [
                {'name': 'id', 'type': 'integer'}
            ]
        },
        'data': [[value,] for value in range(0, 15000)]
    }

    # Write data
    storage = Storage(SERVICE, project=PROJECT, dataset=DATASET, prefix=PREFIX)
    storage.create('bucket', RESOURCE['schema'], force=True)
    storage.write('bucket', RESOURCE['data'])

    # Pull rows
    # TODO: remove sorting after proper soring solution implementation
    assert sorted(storage.read('bucket'), key=lambda row: row[0]) == RESOURCE['data']


# Helpers

def cast(resource, skip=[]):
    resource = deepcopy(resource)
    schema = tableschema.Schema(resource['schema'])
    for row in resource['data']:
        for index, field in enumerate(schema.fields):
            if field.type not in skip:
                row[index] = field.cast_value(row[index])
    return resource
