# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import io
import json
import runpy
import tempfile
import unittest


class TestResource(unittest.TestCase):

    # Helpers

    def setUp(self):

        # Import files
        datadir = os.path.join(
                os.path.dirname(__file__), '..', '..', 'examples', 'data', 'spending')
        self.import_schema_path = os.path.join(datadir, 'schema.json')
        self.import_data_path = os.path.join(datadir, 'data.csv')

        # Export files
        _, self.export_schema_path = tempfile.mkstemp()
        _, self.export_data_path = tempfile.mkstemp()

        # Update environ
        os.environ['IMPORT_SCHEMA_PATH'] = self.import_schema_path
        os.environ['EXPORT_SCHEMA_PATH'] = self.export_schema_path
        os.environ['IMPORT_DATA_PATH'] = self.import_data_path
        os.environ['EXPORT_DATA_PATH'] = self.export_data_path

    def tearDown(self):

        # Delete temp files
        try:
            os.remove(self.export_schema_path)
            os.remove(self.export_data_path)
        except Exception:
            pass

    # Tests

    def test(self):

        # Run example
        runpy.run_module('examples.resource')

        # Assert schema
        actual = json.load(io.open(self.export_schema_path, encoding='utf-8'))
        expected = json.load(io.open(self.import_schema_path, encoding='utf-8'))
        assert actual == expected

        # Assert data
        # TODO: parse csv
        actual = io.open(self.export_data_path, encoding='utf-8').read()
        expected = io.open(self.import_data_path, encoding='utf-8').read()
        assert actual == expected
