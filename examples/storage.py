# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import io
import json
import uuid
from tabulator import topen
from apiclient.discovery import build
from oauth2client.client import GoogleCredentials

from jsontableschema_bigquery import Storage


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

# List tables
print(storage.tables)

# Describe tables
print(storage.describe('articles'))

# Read data from tables
print(list(storage.read('articles')))

# Delete tables
for table in reversed(storage.tables):
    storage.delete(table)
