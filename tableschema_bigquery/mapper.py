# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import re
import json
import tableschema
from slugify import slugify
from dateutil.parser import parse


# Module API

class Mapper(object):

    # Public

    def __init__(self, prefix):
        """Mapper to convert/restore FD entities to/from BigQuery entities
        """
        self.__prefix = prefix

    def convert_bucket(self, bucket):
        """Convert bucket to BigQuery
        """
        return self.__prefix + bucket

    def convert_descriptor(self, descriptor):
        """Convert descriptor to BigQuery
        """

        # Fields
        fields = []
        fallbacks = []
        schema = tableschema.Schema(descriptor)
        for index, field in enumerate(schema.fields):
            converted_type = self.convert_type(field.type)
            if not converted_type:
                converted_type = 'STRING'
                fallbacks.append(index)
            mode = 'NULLABLE'
            if field.required:
                mode = 'REQUIRED'
            fields.append({
                'name': _slugify_field_name(field.name),
                'type': converted_type,
                'mode': mode,
            })

        # Descriptor
        converted_descriptor = {
            'fields': fields,
        }

        return (converted_descriptor, fallbacks)

    def convert_row(self, row, schema, fallbacks):
        """Convert row to BigQuery
        """
        for index, field in enumerate(schema.fields):
            value = row[index]
            if index in fallbacks:
                value = _uncast_value(value, field=field)
            else:
                value = field.cast_value(value)
            row[index] = value
        return row

    def convert_type(self, type):
        """Convert type to BigQuery
        """

        # Mapping
        mapping = {
            'any': 'STRING',
            'array': None,
            'boolean': 'BOOLEAN',
            'date': 'DATE',
            'datetime': 'DATETIME',
            'duration': None,
            'geojson': None,
            'geopoint': None,
            'integer': 'INTEGER',
            'number': 'FLOAT',
            'object': None,
            'string': 'STRING',
            'time': 'TIME',
            'year': 'INTEGER',
            'yearmonth': None,
        }

        # Not supported type
        if type not in mapping:
            message = 'Type %s is not supported' % type
            raise tableschema.exceptions.StorageError(message)

        return mapping[type]

    def restore_bucket(self, table_name):
        """Restore bucket from BigQuery
        """
        if table_name.startswith(self.__prefix):
            return table_name.replace(self.__prefix, '', 1)
        return None

    def restore_descriptor(self, converted_descriptor):
        """Restore descriptor rom BigQuery
        """

        # Convert
        fields = []
        for field in converted_descriptor['fields']:
            field_type = self.restore_type(field['type'])
            resfield = {
                'name': field['name'],
                'type': field_type,
            }
            if field.get('mode', 'NULLABLE') != 'NULLABLE':
                resfield['constraints'] = {'required': True}
            fields.append(resfield)
        descriptor = {'fields': fields}

        return descriptor

    def restore_row(self, row, schema):
        """Restore row from BigQuery
        """
        for index, field in enumerate(schema.fields):
            if field.type == 'datetime':
                row[index] = parse(row[index])
            if field.type == 'date':
                row[index] = parse(row[index]).date()
            if field.type == 'time':
                row[index] = parse(row[index]).time()
        return schema.cast_row(row)

    def restore_type(self, type):
        """Restore type from BigQuery
        """

        # Mapping
        mapping = {
            'BOOLEAN': 'boolean',
            'DATE': 'date',
            'DATETIME': 'datetime',
            'INTEGER': 'integer',
            'FLOAT': 'number',
            'STRING': 'string',
            'TIME': 'time',
        }

        # Not supported type
        if type not in mapping:
            message = 'Type %s is not supported' % type
            raise tableschema.exceptions.StorageError(message)

        return mapping[type]


# Internal

def _slugify_field_name(name):

    # Referene:
    # https://cloud.google.com/bigquery/docs/reference/v2/tables
    MAX_LENGTH = 128
    VALID_NAME = r'^[a-zA-Z_]\w{0,%d}$' % (MAX_LENGTH-1)

    # Convert
    if not re.match(VALID_NAME, name):
        name = slugify(name, separator='_')
        if not re.match('^[a-zA-Z_]', name):
            name = '_' + name

    return name[:MAX_LENGTH]


def _uncast_value(value, field):
    # Eventially should be moved to:
    # https://github.com/frictionlessdata/tableschema-py/issues/161
    if isinstance(value, (list, dict)):
        value = json.dumps(value)
    else:
        value = str(value)
    return value
