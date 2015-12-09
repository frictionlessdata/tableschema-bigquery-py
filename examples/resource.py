# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os
from jtsbq import Resource
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials


# Parameters
client_email = os.environ['GOOGLE_CLIENT_EMAIL']
private_key = os.environ['GOOGLE_PRIVATE_KEY']
scope = 'https://www.googleapis.com/auth/bigquery'

# Service
credentials = SignedJwtAssertionCredentials(client_email, private_key, scope)
service = build('bigquery', 'v2', credentials=credentials)

# Table
resource = Resource(service, 'frictionless-data-test', 'jsontableschema', 'download')
# resource.add_data([('roll', 'roll', 'roll', 333.0)])
data = resource.get_data()
print(resource.schema)
print(list(data))
resource.export_schema('tmp/schema.json')
resource.export_data('tmp/data.csv')
resource.import_data('examples/data/spending/data.csv')
