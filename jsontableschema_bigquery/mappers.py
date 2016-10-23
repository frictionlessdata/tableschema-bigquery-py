# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import re
from slugify import slugify


# Module API

def bucket_to_tablename(prefix, bucket):
    """Convert bucket to Bigquery tablename.
    """
    return prefix + bucket


def tablename_to_bucket(prefix, tablename):
    """Convert Bigquery tablename to bucket.
    """
    if tablename.startswith(prefix):
        return tablename.replace(prefix, '', 1)
    return None


def descriptor_to_nativedesc(descriptor):
    """Convert descriptor to BigQuery nativedesc.
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

    # Convert
    fields = []
    for field in descriptor['fields']:
        try:
            ftype = mapping[field['type']]
        except KeyError:
            message = 'Type %s is not supported' % field['type']
            raise TypeError(message)
        mode = 'NULLABLE'
        if field.get('constraints', {}).get('required', False):
            mode = 'REQUIRED'
        fields.append({
            'name': _slugify_field_name(field['name']),
            'type': ftype,
            'mode': mode,
        })
    nativedesc = {'fields': fields}

    return nativedesc


def nativedesc_to_descriptor(nativedesc):
    """Convert BigQuery nativedesc to descriptor.
    """

    # Mapping
    mapping = {
        'STRING': 'string',
        'INTEGER': 'integer',
        'FLOAT': 'number',
        'BOOLEAN': 'boolean',
        'TIMESTAMP': 'datetime',
    }

    # Convert
    fields = []
    for field in nativedesc['fields']:
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
    descriptor = {'fields': fields}

    return descriptor


# Internal

def _slugify_field_name(name):

    # Referene:
    # https://cloud.google.com/bigquery/docs/reference/v2/tables
    MAX_LENGTH = 128
    VALID_NAME = '^[a-zA-Z_]\w{0,%d}$' % (MAX_LENGTH-1)

    # Convert
    if not re.match(VALID_NAME, name):
        name = slugify(name, separator='_')
        if not re.match('^[a-zA-Z_]', name):
            name = '_' + name

    return name[:MAX_LENGTH]
