# tableschema-bigquery-py

[![Travis](https://img.shields.io/travis/frictionlessdata/tableschema-bigquery-py/master.svg)](https://travis-ci.org/frictionlessdata/tableschema-bigquery-py)
[![Coveralls](http://img.shields.io/coveralls/frictionlessdata/tableschema-bigquery-py.svg?branch=master)](https://coveralls.io/r/frictionlessdata/tableschema-bigquery-py?branch=master)
[![PyPi](https://img.shields.io/pypi/v/tableschema-bigquery.svg)](https://pypi.python.org/pypi/tableschema-bigquery)
[![Gitter](https://img.shields.io/gitter/room/frictionlessdata/chat.svg)](https://gitter.im/frictionlessdata/chat)

Generate and load BigQuery tables based on [Table Schema](http://specs.frictionlessdata.io/table-schema/) descriptors.

## Features

- implements `tableschema.Storage` interface

## Getting Started

### Installation

The package use semantic versioning. It means that major versions  could include breaking changes. It's highly recommended to specify `package` version range in your `setup/requirements` file e.g. `package>=1.0,<2.0`.

```bash
pip install tableschema-bigquery
```

To start using Google BigQuery service:
- Create a new project - [link](https://console.developers.google.com/home/dashboard)
- Create a service key - [link](https://console.developers.google.com/apis/credentials)
- Download json credentials and set `GOOGLE_APPLICATION_CREDENTIALS` environment variable

### Examples

Code examples in this readme requires Python 3.3+ interpreter. You could see even more example in [examples](https://github.com/frictionlessdata/tableschema-bigquery-py/tree/master/examples) directory.

```python
import io
import os
import json
from tableschema import Table
from apiclient.discovery import build
from oauth2client.client import GoogleCredentials

# Prepare BigQuery credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '.credentials.json'
credentials = GoogleCredentials.get_application_default()
service = build('bigquery', 'v2', credentials=credentials)
project = json.load(io.open('.credentials.json', encoding='utf-8'))['project_id']

# Load and save table to BigQuery
table = Table('data.csv', schema='schema.json')
table.save('data', storage='bigquery', service=service, project=project, dataset='dataset')
```

## Documentation

The whole public API of this package is described here and follows semantic versioning rules. Everyting outside of this readme are private API and could be changed without any notification on any new version.

### Storage

Package implements [Tabular Storage](https://github.com/frictionlessdata/tableschema-py#storage) interface (see full documentation on the link):

![Storage](https://i.imgur.com/RQgrxqp.png)

This driver provides an additional API:

#### `Storage(service, project, dataset, prefix='')`

- `service (object)` - BigQuery `Service` object
- `project (str)` - BigQuery project name
- `dataset (str)` - BigQuery dataset name
- `prefix (str)` - prefix for all buckets

## Contributing

The project follows the [Open Knowledge International coding standards](https://github.com/okfn/coding-standards).

Recommended way to get started is to create and activate a project virtual environment.
To install package and development dependencies into active environment:

```
$ make install
```

To run tests with linting and coverage:

```bash
$ make test
```

For linting `pylama` configured in `pylama.ini` is used. On this stage it's already
installed into your environment and could be used separately with more fine-grained control
as described in documentation - https://pylama.readthedocs.io/en/latest/.

For example to sort results by error type:

```bash
$ pylama --sort <path>
```

For testing `tox` configured in `tox.ini` is used.
It's already installed into your environment and could be used separately with more fine-grained control as described in documentation - https://testrun.org/tox/latest/.

For example to check subset of tests against Python 2 environment with increased verbosity.
All positional arguments and options after `--` will be passed to `py.test`:

```bash
tox -e py27 -- -v tests/<path>
```

Under the hood `tox` uses `pytest` configured in `pytest.ini`, `coverage`
and `mock` packages. This packages are available only in tox envionments.

## Changelog

Here described only breaking and the most important changes. The full changelog and documentation for all released versions could be found in nicely formatted [commit history](https://github.com/frictionlessdata/tableschema-bigquery-py/commits/master).

### v0.x

Initial driver implementation.
