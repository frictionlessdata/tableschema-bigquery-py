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
from jsontableschema_bigquery import Storage


def run(dataset, prefix, table, schema, data):

    # Storage
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '.credentials.json'
    credentials = GoogleCredentials.get_application_default()
    service = build('bigquery', 'v2', credentials=credentials)
    project = json.load(io.open('.credentials.json', encoding='utf-8'))['project_id']
    storage = Storage(service, project, dataset, prefix=prefix)

    # Check table
    if storage.check(table):
        # Delete table
        storage.delete(table)

    # Create table
    storage.create(table, schema)

    # Write data to table
    storage.write(table, data)

    # List tables
    tables = storage.tables

    # Describe table
    schema = storage.describe(table)

    # Read data from table
    data = list(storage.read(table))

    return tables, schema, data
