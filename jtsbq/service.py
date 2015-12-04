# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import uuid
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials


class ServiceTable(object):

    # Public

    SCOPE = ['https://www.googleapis.com/auth/bigquery']

    def __init__(self, client_email, private_key, project_id, dataset_id, table_id):
        self.__client_email = client_email
        self.__private_key = private_key
        self.__project_id = project_id
        self.__dataset_id = dataset_id
        self.__table_id = table_id

    def download(self, path, num_retries=5):
        credentials = SignedJwtAssertionCredentials(
                self.__client_email, self.__private_key, self.SCOPE)
        bigquery = build('bigquery', 'v2', credentials=credentials)
        template = 'SELECT * FROM [{project_id}:{dataset_id}.{table_id}];'
        query = template.format(
                project_id=self.__project_id,
                dataset_id=self.__dataset_id,
                table_id=self.__table_id)
        response = bigquery.jobs().query(
            projectId=self.__project_id,
            body={'query': query}).execute()
        for row in response['rows']:
            print(','.join(field['v'] for field in row['f']))
