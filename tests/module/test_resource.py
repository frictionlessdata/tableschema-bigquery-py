# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import json
import pytest
import unittest
from mock import MagicMock, patch
from importlib import import_module
module = import_module('jtsbq.resource')


class TestResource(unittest.TestCase):

    # Helpers

    def setUp(self):

        # Fixtures
        self.resource_schema = {
            'fields': [
                {'name': 'id', 'type': 'integer'},
                {'name': 'name', 'type': 'string'},
            ]
        }
        self.table_schema = {
            'fields': [
                {'name': 'id', 'type': 'INTEGER'},
                {'name': 'name', 'type': 'STRING'},
            ]
        }
        self.resource_headers = ('id', 'name')
        self.resource_data = [('1', 'name1'), ('2', 'name2')]
        self.table_data = [(1, 'name1'), (2, 'name2')]

        # Mocks
        self.addCleanup(patch.stopall)
        self.Table = patch.object(module, 'Table').start()
        self.table = self.Table.return_value
        self.service = MagicMock()
        self.project_id = 'project_id'
        self.dataset_id = 'dataset_id'
        self.table_id = 'table_id'

        # Create resource
        self.resource = module.Resource(
                service=self.service,
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table_id=self.table_id)

    # Tests

    def test_table(self):

        # Assert values
        assert self.resource.table == self.table

    def test_is_existent_true(self):

        # Assert values
        assert self.resource.is_existent

    def test_is_existent_false(self):

        # Mocks
        self.table.is_existent = False

        # Assert values
        assert not self.table.is_existent

    def test_create(self):

        # Method call
        self.resource.create(self.resource_schema)

        # Assert calls
        self.table.create.assert_called_with(self.table_schema)

    def test_delete(self):

        # Method call
        self.resource.delete()

        # Assert calls
        self.table.delete.assert_called_with()

    def test_schema(self):

        # Mocks
        self.table.schema = self.table_schema

        # Assert values
        self.resource.schema == self.resource_schema

    def test_add_data(self):

        # Mocks
        self.table.schema = self.table_schema

        # Method call
        self.resource.add_data(self.resource_data)

        # Assert calls
        self.table.add_data.assert_called_with(self.table_data)

    def test_get_data(self):

        # Mocks
        self.table.schema = self.table_schema
        self.table.get_data.return_value = self.table_data

        # Assert values
        list(self.resource.get_data()) == self.resource_data

    def test_import_data(self):

        # Mocks
        self.table.schema = self.table_schema
        source = 'text://%s' % json.dumps([self.resource_headers] + self.resource_data)

        # Method call
        self.resource.import_data(source, format='json')

        # Assert calls
        self.table.add_data.assert_called_with(self.table_data)

    def test_export_schema(self):
        pass

    def test_export_data(self):
        pass
