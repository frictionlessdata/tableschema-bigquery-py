# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import csv
import six
import json
from tabulator import topen, processors
from jsontableschema.model import SchemaModel

from . import schema as schema_module
from .table import Table


# Module API

class Resource(object):
    """Data resource representation.
    """

    # Public

    def __init__(self, service, project_id, dataset_id, table_id,
                 schema=None):

        # Convert schema
        if schema is not None:
            schema = schema_module.resource2table(schema)

        # Create table
        self.__table = Table(
                service=service,
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id,
                schema=schema)

    @property
    def schema(self):
        """Return schema.
        """

        # Create cache
        if not hasattr(self, '__schema'):

            # Get and convert schema
            schema = self.__table.schema
            schema = schema_module.table2resource(schema)
            self.__schema = schema

        return self.__schema

    def get_data(self):
        """Return data generator.
        """

        # Get model and data
        model = SchemaModel(self.schema)
        data = self.__table.get_data()

        # Yield converted data
        for row in data:
            row = tuple(model.convert_row(*row))
            yield row

    def add_data(self, data):
        """Add data to resource.
        """

        # Get model and data
        model = SchemaModel(self.schema)
        cdata = []
        for row in data:
            row = tuple(model.convert_row(*row))
            cdata.append(row)

        # Add data to table
        self.__table.add_data(cdata)

    def export_schema(self, path):
        """Export schema to file.
        """

        # Write dump on disk
        with io.open(path,
                     mode=self.__write_mode,
                     encoding=self.__write_encoding) as file:
            json.dump(self.schema, file, indent=4)

    def export_data(self, path):
        """Export data to file.
        """

        # Get model
        model = SchemaModel(self.schema)

        # Write csv on disk
        with io.open(path,
                     mode=self.__write_mode,
                     newline=self.__write_newline,
                     encoding=self.__write_encoding) as file:
            writer = csv.writer(file)
            writer.writerow(model.headers)
            for row in self.get_data():
                writer.writerow(row)

    def import_data(self, path, **options):
        """Import data from file.
        """

        # Get data
        data = []
        with topen(path, **options) as table:
            # TODO: add header row config?
            table.add_processor(processors.Headers())
            table.add_processor(processors.Schema(self.schema))
            for row in table.readrow():
                data.append(row)

        # Add data to table
        self.__table.add_data(data)

    # Private

    @property
    def __write_mode(self):
        if six.PY2:
            return 'wb'
        return 'w'

    @property
    def __write_encoding(self):
        if six.PY2:
            return None
        return 'utf-8'

    @property
    def __write_newline(self):
        if six.PY2:
            return None
        return ''
