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


def run(import_schema='examples/data/spending/schema.json',
        export_schema='tmp/schema.json',
        import_data='examples/data/spending/data.csv',
        export_data='tmp/data.csv',
        dataset='jsontableschema',
        prefix='test_',
        table='resource_tests'):

    # Service
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '.credentials.json'
    credentials = GoogleCredentials.get_application_default()
    service = build('bigquery', 'v2', credentials=credentials)

    # Table
    project = json.load(io.open('.credentials.json', encoding='utf-8'))['project_id']
    storage = jtsbq.Storage(service, project, dataset, prefix=prefix)

    # Import
    print('[Import]')
    jtsbq.import_resource(
            storage=storage,
            table=table,
            schema=import_schema,
            data=import_data,
            force=True)
    print('imported')

    # Export
    print('[Export]')
    jtsbq.export_resource(
            storage=storage,
            table=table,
            schema=export_schema,
            data=export_data)
    print('exported')

    return locals()


if __name__ == '__main__':
    run()
