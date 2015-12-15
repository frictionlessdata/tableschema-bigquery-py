# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os
from jtsbq import Resource
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials

# Arguments
import_schema_path = os.getenv('IMPORT_SCHEMA_PATH', 'examples/data/spending/schema.json')
export_schema_path = os.getenv('EXPORT_SCHEMA_PATH', 'tmp/schema.json')
import_data_path = os.getenv('IMPORT_DATA_PATH', 'examples/data/spending/data.csv')
export_data_path = os.getenv('EXPORT_DATA_PATH', 'tmp/data.csv')
table_id = os.getenv('TABLE_ID', 'resource_test')

# Parameters
client_email = os.environ['GOOGLE_CLIENT_EMAIL']
private_key = os.environ['GOOGLE_PRIVATE_KEY']
project_id = os.environ['GOOGLE_PROJECT_ID']
scope = 'https://www.googleapis.com/auth/bigquery'

# Service
credentials = SignedJwtAssertionCredentials(client_email, private_key, scope)
service = build('bigquery', 'v2', credentials=credentials)

# Resource
resource = Resource(service, project_id, 'jsontableschema', table_id)

# Delete
print('[Delete]')
print(resource.is_existent)
if resource.is_existent:
    resource.delete()

# Create
print('[Create]')
print(resource.is_existent)
resource.create(import_schema_path)
print(resource.is_existent)
print(resource.schema)

# Add data
# print('[Add data]')
# resource.add_data([('id1', 'name1', True, 333.0)])
# print(list(resource.get_data()))

# Import data
print('[Import data]')
resource.import_data(import_data_path)
print(list(resource.get_data()))

# Export schema/data
print('[Export schema/data]')
resource.export_schema(export_schema_path)
resource.export_data(export_data_path)
print('done')
