# jsontableschema-bigquery-py

[![Travis](https://img.shields.io/travis/frictionlessdata/jsontableschema-bigquery-py.svg)](https://travis-ci.org/frictionlessdata/jsontableschema-bigquery-py)
[![Coveralls](http://img.shields.io/coveralls/frictionlessdata/jsontableschema-bigquery-py.svg?branch=master)](https://coveralls.io/r/frictionlessdata/jsontableschema-bigquery-py?branch=master)
[![PyPi](https://img.shields.io/pypi/v/jsontableschema-bigquery.svg)](https://pypi.python.org/pypi/jsontableschema-bigquery)
[![Gitter](https://img.shields.io/gitter/room/frictionlessdata/chat.svg)](https://gitter.im/frictionlessdata/chat)

Generate and load BigQuery tables based on JSON Table Schema descriptors.

## Tabular Storage

Package implements [Tabular Storage](https://github.com/okfn/datapackage-storage-py#tabular-storage) interface.

To start using Google BigQuery service:
- Create a new project - [link](https://console.developers.google.com/home/dashboard)
- Create a service key - [link](https://console.developers.google.com/apis/credentials)
- Download json credentials and set `GOOGLE_APPLICATION_CREDENTIALS` environment variable

We can get storage this way:

```python
import io
import os
import json
from apiclient.discovery import build
from oauth2client.client import GoogleCredentials
from jsontableschema_bigquery import Storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '.credentials.json'
credentials = GoogleCredentials.get_application_default()
service = build('bigquery', 'v2', credentials=credentials)
project = json.load(io.open('.credentials.json', encoding='utf-8'))['project_id']
storage = Storage(service, project, 'dataset', prefix='prefix')
```

Then we could interact with storage:

```python
storage.tables
storage.check('table_name') # check existence
storage.create('table_name', schema)
storage.delete('table_name')
storage.describe('table_name') # return schema
storage.read('table_name') # return data
storage.write('table_name', data)
```

## Mappings

```
schema.json -> bigquery table schema
data.csv -> bigquery talbe data
```

## Drivers

Default Google BigQuery client is used - [docs](https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/python/latest/).

## Documentation

API documentation is presented as docstings:
- [Storage](https://github.com/frictionlessdata/jsontableschema-bigquery-py/blob/master/jsontableschema_bigquery/storage.py)

## Contributing

Please read the contribution guideline:

[How to Contribute](CONTRIBUTING.md)

Thanks!
