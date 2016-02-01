# -*- coding: utf-8 -*-
import datetime
import platform
import random
import string

from riak.table import Table
from riak.transports.ttb.codec import RiakTtbCodec
from riak.util import str_to_bytes, bytes_to_str, unix_time_millis

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest

table_name = 'GeoCheckin'

bd0 = '时间序列'
bd1 = 'временные ряды'

fiveMins = datetime.timedelta(0, 300)
ts0 = datetime.datetime(2015, 1, 1, 12, 0, 0)
ts1 = ts0 + fiveMins


class TimeseriesTtbUnitTests(unittest.TestCase):
    def setUp(self):
        self.c = RiakTtbCodec()
        self.ts0ms = unix_time_millis(ts0)
        self.ts1ms = unix_time_millis(ts1)
        self.rows = [
            [bd0, 0, 1.2, ts0, True],
            [bd1, 3, 4.5, ts1, False]
        ]
        self.test_key = ['hash1', 'user2', ts0]
        self.table = Table(None, table_name)

    def test_encode_data_for_get(self):
        req = self.c._encode_timeseries_keyreq(self.table, self.test_key)
        self.assertIsNotNone(req)
