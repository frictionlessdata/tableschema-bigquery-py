# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import re
from slugify import slugify


# Module API

def convert_table(prefix, table):
    """Convert high-level table name to database name.
    """
    return prefix + table


def restore_table(prefix, table):
    """Restore database table name to high-level name.
    """
    if table.startswith(prefix):
        return table.replace(prefix, '', 1)
    return None


def convert_schema(schema):
    """Convert JSONTableSchema schema to BigQuery schema.
    """

    # Mapping
    mapping = {
        'string': 'STRING',
        'integer': 'INTEGER',
        'number': 'FLOAT',
        'boolean': 'BOOLEAN',
        'date': 'TIMESTAMP',
        'time': 'TIMESTAMP',
        'datetime': 'TIMESTAMP',
    }

    # Schema
    fields = []
    for field in schema['fields']:
        try:
            ftype = mapping[field['type']]
        except KeyError:
            message = 'Type %s is not supported' % field['type']
            raise TypeError(message)
        mode = 'NULLABLE'
        if field.get('constraints', {}).get('required', False):
            mode = 'REQUIRED'
        resfield = {
            'name': convert_field_name(field['name']),
            'type': ftype,
            'mode': mode,
        }
        fields.append(resfield)
    schema = {'fields': fields}

    return schema


def restore_schema(schema):
    """Convert BigQuery schema to JSONTableSchema schema.
    """

    # Mapping
    mapping = {
        'STRING': 'string',
        'INTEGER': 'integer',
        'FLOAT': 'number',
        'BOOLEAN': 'boolean',
        'TIMESTAMP': 'datetime',
    }

    # Schema
    fields = []
    for field in schema['fields']:
        try:
            ftype = mapping[field['type']]
        except KeyError:
            message = 'Type %s is not supported' % field['type']
            raise TypeError(message)
        resfield = {
            'name': field['name'],
            'type': ftype,
        }
        if field.get('mode', 'NULLABLE') != 'NULLABLE':
            resfield['constraints'] = {'required': True}
        fields.append(resfield)
    schema = {'fields': fields}

    return schema


def convert_field_name(name):
    # Check https://cloud.google.com/bigquery/docs/reference/v2/tables for
    # reference
    MAX_LENGTH = 128
    VALID_NAME = '^[a-zA-Z_]\w{0,%d}$' % (MAX_LENGTH-1)

    if not re.match(VALID_NAME, name):
        name = slugify(name, separator='_')
        if not re.match('^[a-zA-Z_]', name):
            name = '_' + name

    return name[:MAX_LENGTH]
