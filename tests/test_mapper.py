# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from tableschema_bigquery.mapper import Mapper


# Tests

def test_mapper_convert_bucket():
    mapper = Mapper('prefix_')
    assert mapper.convert_bucket('bucket') == 'prefix_bucket'


def test_mapper_restore_bucket():
    mapper = Mapper('prefix_')
    assert mapper.restore_bucket('prefix_bucket') == 'bucket'
    assert mapper.restore_bucket('xxxxxx_bucket') == None
