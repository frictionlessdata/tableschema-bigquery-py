# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import argparse

from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.client import GoogleCredentials, SignedJwtAssertionCredentials


def main(project_id):
    # [START build_service]
    # Grab the application's default credentials from the environment.
    credentials = SignedJwtAssertionCredentials(os.environ['GOOGLE_CLIENT_EMAIL'], os.environ['GOOGLE_PRIVATE_KEY'], ['https://www.googleapis.com/auth/bigquery'])
    # Construct the service object for interacting with the BigQuery API.
    bigquery_service = build('bigquery', 'v2', credentials=credentials)
    # [END build_service]

    try:
        # [START run_query]
        query_request = bigquery_service.jobs()
        query_data = {
            'query': (
                'SELECT TOP(corpus, 10) as title, '
                'COUNT(*) as unique_words '
                'FROM [publicdata:samples.shakespeare];')
        }

        query_response = query_request.query(
            projectId=project_id,
            body=query_data).execute()
        # [END run_query]

        # [START print_results]
        print('Query Results:')
        for row in query_response['rows']:
            print('\t'.join(field['v'] for field in row['f']))
        # [END print_results]

    except HttpError as err:
        print('Error: {}'.format(err.content))
        raise err


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('project_id', help='Your Google Cloud Project ID.')

    args = parser.parse_args()

    main(args.project_id)
