# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os
from jtsbq import Table
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials


# Parameters
client_email = os.environ['GOOGLE_CLIENT_EMAIL']
private_key = os.environ['GOOGLE_PRIVATE_KEY']
project_id = os.environ['GOOGLE_PROJECT_ID']
scope = 'https://www.googleapis.com/auth/bigquery'

# Service
credentials = SignedJwtAssertionCredentials(client_email, private_key, scope)
service = build('bigquery', 'v2', credentials=credentials)

# Table
table = Table(service, project_id, 'jsontableschema', 'table_test')

# Delete
print('[Delete]')
print(table.is_existent)
if table.is_existent:
    table.delete()

# Create
print('[Create]')
print(table.is_existent)
table.create({'fields': [{'name': 'id', 'type': 'STRING'}]})
print(table.is_existent)
print(table.schema)

# Add data
print('[Add data]')
table.add_data([('id1',), ('id2',)])
print(list(table.get_data()))
