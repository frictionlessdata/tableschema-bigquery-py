# jsontableschema-bigquery-py

[![Travis](https://img.shields.io/travis/okfn/jsontableschema-bigquery-py.svg)](https://travis-ci.org/okfn/jsontableschema-bigquery-py)
[![Coveralls](http://img.shields.io/coveralls/okfn/jsontableschema-bigquery-py.svg?branch=master)](https://coveralls.io/r/okfn/jsontableschema-bigquery-py?branch=master)

Generate and load BigQuery tables based on JSON Table Schema descriptors.

## Usage

This section is intended to be used by end-users of the library.

### Import/Export

> See section below how to get authentificated service.

High-level API is easy to use.

Having `schema.json` (JSONTableSchema) and `data.csv` in
current directory we can import it to bigquery database:

```python
import jtssql

storage = jtsbq.Storage(<service>, project, dataset)
jtsbq.import_resource(storage, 'table', 'schema.json', 'data.csv')
```

Also we can export it from bigquery database:

```python
import jtsbq

storage = jtsbq.Storage(<service>, project, dataset)
jtsbq.export_resource(storage, 'table', 'schema.json', 'data.csv')
```

### Authentificated service

To start using Google BigQuery service:
- Create a new project - [link](https://console.developers.google.com/home/dashboard)
- Create a service key - [link](https://console.developers.google.com/apis/credentials)
- Download json credentials and set `GOOGLE_APPLICATION_CREDENTIALS` environment variable

For example:

```python
import os
from apiclient.discovery import build
from oauth2client.client import GoogleCredentials

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '.credentials.json'
credentials = GoogleCredentials.get_application_default()
service = build('bigquery', 'v2', credentials=credentials)
```

### Design Overview

#### Storage

On level between the high-level interface and bigquery driver
package uses **Tabular Storage** concept:

![Tabular Storage](diagram.png)

#### Mappings

```
schema.json -> bigquery table schema
data.csv -> bigquery talbe data
```

#### Drivers

Default Google BigQuery client is used - [docs](https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/python/latest/).

### Documentation

API documentation is presented as docstings:
- [Resource](https://github.com/okfn/jsontableschema-bigquery-py/blob/master/jtsbq/resource.py)
- [Table](https://github.com/okfn/jsontableschema-bigquery-py/blob/master/jtsbq/table.py)

## Development

This section is intended to be used by tech users collaborating
on this project.

### Getting Started

To activate virtual environment, install
dependencies, add pre-commit hook to review and test code
and get `run` command as unified developer interface:

```
$ source activate.sh
```

### Reviewing

The project follow the next style guides:
- [Open Knowledge Coding Standards and Style Guide](https://github.com/okfn/coding-standards)
- [PEP 8 - Style Guide for Python Code](https://www.python.org/dev/peps/pep-0008/)

To check the project against Python style guide:

```
$ run review
```

### Testing

To run tests with coverage check:

```
$ run test
```

Coverage data will be in the `.coverage` file.
