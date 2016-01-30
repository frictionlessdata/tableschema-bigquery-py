# jsontableschema-bigquery-py

[![Travis](https://img.shields.io/travis/okfn/jsontableschema-bigquery-py.svg)](https://travis-ci.org/okfn/jsontableschema-bigquery-py)
[![Coveralls](http://img.shields.io/coveralls/okfn/jsontableschema-bigquery-py.svg?branch=master)](https://coveralls.io/r/okfn/jsontableschema-bigquery-py?branch=master)

Generate and load BigQuery tables based on JSON Table Schema descriptors.

## Import/Export

> See section below how to get tabular storage object.

High-level API is easy to use.

Having `schema.json` (JSONTableSchema) and `data.csv` in
current directory we can import it to bigquery database:

```python
import jtssql

jtsbq.import_resource(<storage>, 'table', 'schema.json', 'data.csv')
```

Also we can export it from bigquery database:

```python
import jtsbq

jtsbq.export_resource(<storage>, 'table', 'schema.json', 'data.csv')
```

## Tabular Storage

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

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '.credentials.json'
credentials = GoogleCredentials.get_application_default()
service = build('bigquery', 'v2', credentials=credentials)
project = json.load(io.open('.credentials.json', encoding='utf-8'))['project_id']
storage = jtsbq.Storage(service, project, 'dataset')
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
- [Resource](https://github.com/okfn/jsontableschema-bigquery-py/blob/master/jtsbq/resource.py)
- [Table](https://github.com/okfn/jsontableschema-bigquery-py/blob/master/jtsbq/table.py)
