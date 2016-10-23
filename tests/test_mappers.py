# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from jsontableschema_bigquery import mappers


# Tests

def test_bucket_to_table():
    assert mappers.bucket_to_table('prefix_', 'bucket') == 'prefix_bucket'


def test_table_to_bucket():
    assert mappers.table_to_bucket('prefix_', 'prefix_bucket') == 'bucket'
    assert mappers.table_to_bucket('prefix_', 'xxxxxx_bucket') == None
