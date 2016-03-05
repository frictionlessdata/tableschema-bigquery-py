# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
import unittest
from mock import MagicMock, patch, ANY
from importlib import import_module
module = import_module('jsontableschema_bigquery.storage')


class TestStorage(unittest.TestCase):

    # Helpers

    def setUp(self):

        # Mocks
        self.addCleanup(patch.stopall)
        self.service = MagicMock()
        self.project = 'project'
        self.dataset = 'dataset'
        self.prefix = 'prefix_'

        # Create storage
        self.storage = module.Storage(
                service=self.service,
                project=self.project,
                dataset=self.dataset,
                prefix=self.prefix)

    # Tests

    def test___repr__(self):

        # Assert values
        assert repr(self.storage)

    def test_tables(self):

        # Mocks
        self.service.tables.return_value.list.return_value.execute.return_value = {
            'tables': [
                {'tableReference': {'tableId': 'prefix_table1'}},
                {'tableReference': {'tableId': 'prefix_table2'}},
            ],
        }

        # Assert values
        assert self.storage.tables == ['table1', 'table2']

    def test_check(self):

        # Mocks
        self.service.tables.return_value.list.return_value.execute.return_value = {
            'tables': [
                {'tableReference': {'tableId': 'prefix_table1'}},
                {'tableReference': {'tableId': 'prefix_table2'}},
            ],
        }

        # Assert values
        assert self.storage.check('table1')
        assert self.storage.check('table2')
        assert not self.storage.check('table3')

    def test_create(self):
        pass

    def test_create_existent(self):

        # Mocks
        self.storage.check = MagicMock(return_value=True)

        # Assert raises
        with pytest.raises(RuntimeError):
            self.storage.create('table', 'schema')

    def test_delete(self):

        # Mocks
        self.storage.check = MagicMock(return_value=True)

        # Method call
        self.storage.delete('table')

        # Assert calls
        self.service.tables.return_value.delete.assert_called_with(
                projectId=self.project,
                datasetId=self.dataset,
                tableId='prefix_table')

    def test_delete_non_existent(self):

        # Assert exception
        with pytest.raises(RuntimeError):
           self.storage.delete('table')

    def test_describe(self):
        pass

    def test_read(self):
        pass

    def test_write(self):
        pass
