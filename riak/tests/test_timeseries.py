# -*- coding: utf-8 -*-
import platform
import time
import sys

from riak.tests import SKIP_TIMESERIES
from riak.tests.base import IntegrationTestBase

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest

class TimeseriesTests(IntegrationTestBase, unittest.TestCase):
    @unittest.skipIf(SKIP_TIMESERIES == '1', "skip requested for timeseries tests")
    def test_store(self):
        pass
        # TODO RTS-367
        # now = int(round(time.time() * 1000)) # NB: millis since Jan 1 1970 UTC
        # fiveMinsInMsec = 5 * 60 * 1000
        # fiveMinsAgo = now - fiveMinsInMsec
        # tenMinsAgo = fiveMinsAgo - fiveMinsInMsec
        # fifteenMinsAgo = tenMinsAgo - fiveMinsInMsec
        # twentyMinsAgo = fifteenMinsAgo - fiveMinsInMsec

        # table = self.client.table(self.table_name)
        # measurements = [
        #     [ 'hash1', 'user2', twentyMinsAgo, 'hurricane', '84.3' ],
        #     [ 'hash1', 'user2', fifteenMinsAgo, 'rain', '79.0' ],
        #     [ 'hash1', 'user2', fiveMinsAgo, 'wind', 50.5 ],
        #     [ 'hash1', 'user2', now, 'snow', 20.1 ]
        # ]
        # ts_obj = table.new(measurements)
        # result = ts_obj.store()
        # self.assertTrue(result)
