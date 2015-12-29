# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import os
import sys
import json
from apiclient.discovery import build
from oauth2client.client import GoogleCredentials

sys.path.insert(0, '.')
import jtsbq


def run(dataset='jsontableschema', table='table_test'):

    # Service
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '.credentials.json'
    credentials = GoogleCredentials.get_application_default()
    service = build('bigquery', 'v2', credentials=credentials)

    # Storage
    project = json.load(io.open('.credentials.json', encoding='utf-8'))['project_id']
    storage = jtsbq.Storage(service, project, dataset)

    # Delete
    print('[Delete]')
    print(storage.check(table))
    if storage.check(table):
        storage.delete(table)

    # Create
    print('[Create]')
    print(storage.check(table))
    storage.create(table, {'fields': [{'name': 'id', 'type': 'string'}]})
    print(storage.check(table))
    print(storage.describe(table))

    # Add data
    print('[Add data]')
    storage.write(table, [('id1',), ('id2',)])
    print(list(storage.read(table)))

    # Tables
    print('[Tables]')
    print(storage.tables)


if __name__ == '__main__':
    run()
