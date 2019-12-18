# tableschema-bigquery-py

[![Travis](https://img.shields.io/travis/frictionlessdata/tableschema-bigquery-py/master.svg)](https://travis-ci.org/frictionlessdata/tableschema-bigquery-py)
[![Coveralls](http://img.shields.io/coveralls/frictionlessdata/tableschema-bigquery-py.svg?branch=master)](https://coveralls.io/r/frictionlessdata/tableschema-bigquery-py?branch=master)
[![PyPi](https://img.shields.io/pypi/v/tableschema-bigquery.svg)](https://pypi.python.org/pypi/tableschema-bigquery)
[![Github](https://img.shields.io/badge/github-master-brightgreen)](https://github.com/frictionlessdata/tableschema-bigquery-py)
[![Gitter](https://img.shields.io/gitter/room/frictionlessdata/chat.svg)](https://gitter.im/frictionlessdata/chat)

Generate and load BigQuery tables based on [Table Schema](http://specs.frictionlessdata.io/table-schema/) descriptors.

## Features

- implements `tableschema.Storage` interface

## Contents

<!--TOC-->

  - [Getting Started](#getting-started)
    - [Installation](#installation)
    - [Prepare BigQuery](#prepare-bigquery)
  - [Documentation](#documentation)
  - [API Reference](#api-reference)
    - [`Storage`](#storage)
  - [Contributing](#contributing)
  - [Changelog](#changelog)

<!--TOC-->

## Getting Started

### Installation

The package use semantic versioning. It means that major versions  could include breaking changes. It's highly recommended to specify `package` version range in your `setup/requirements` file e.g. `package>=1.0,<2.0`.

```bash
pip install tableschema-bigquery
```

### Prepare BigQuery

To start using Google BigQuery service:
- Create a new project - [link](https://console.developers.google.com/home/dashboard)
- Create a service key - [link](https://console.developers.google.com/apis/credentials)
- Download json credentials and set `GOOGLE_APPLICATION_CREDENTIALS` environment variable

## Documentation

```python
import io
import os
import json
from datapackage import Package
from apiclient.discovery import build
from oauth2client.client import GoogleCredentials

# Prepare BigQuery credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '.credentials.json'
credentials = GoogleCredentials.get_application_default()
service = build('bigquery', 'v2', credentials=credentials)
project = json.load(io.open('.credentials.json', encoding='utf-8'))['project_id']

# Save package to BigQuery
package = Package('datapackage.json')
package.save(storage='bigquery', service=service, project=project, dataset='dataset')

# Load package from BigQuery
package = Package(storage='bigquery', service=service, project=project, dataset='dataset')
package.resources
```

## API Reference

### `Storage`
```python
Storage(self, service, project, dataset, prefix='')
```
BigQuery storage

Package implements
[Tabular Storage](https://github.com/frictionlessdata/tableschema-py#storage)
interface (see full documentation on the link):

![Storage](https://i.imgur.com/RQgrxqp.png)

> Only additional API is documented

__Arguments__
- __service (object)__: BigQuery `Service` object
- __project (str)__: BigQuery project name
- __dataset (str)__: BigQuery dataset name
- __prefix (str)__: prefix for all buckets


## Contributing

> The project follows the [Open Knowledge International coding standards](https://github.com/okfn/coding-standards).

Recommended way to get started is to create and activate a project virtual environment.
To install package and development dependencies into active environment:

```bash
$ make install
```

To run tests with linting and coverage:

```bash
$ make test
```

## Changelog

Here described only breaking and the most important changes. The full changelog and documentation for all released versions could be found in nicely formatted [commit history](https://github.com/frictionlessdata/tableschema-bigquery-py/commits/master).

#### v1.0

- Initial driver realease
