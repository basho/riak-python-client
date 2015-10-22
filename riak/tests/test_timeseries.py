# -*- coding: utf-8 -*-
import platform

from . import SKIP_TIMESERIES

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest


class TimeseriesTests(BaseTestCase, unittest.TestCase):

    @unittest.skipIf(SKIP_TIMESERIES == '1', "skip requested for timeseries tests")
    def test_store(self):
        table = self.client.table(self.table_name)
        measurements = [
            [ ]
        ]
        obj = table.new(measurements)
        result = obj.store()
        self.assertTrue(result)
