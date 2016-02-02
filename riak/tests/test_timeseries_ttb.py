# -*- coding: utf-8 -*-
import datetime
import platform
import random
import string

from erlastic import decode, encode
from erlastic.types import Atom

from riak.table import Table
from riak.ts_object import TsObject
from riak.transports.ttb.codec import RiakTtbCodec
from riak.util import str_to_bytes, unix_time_millis

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest

udef_a = Atom('undefined')
tsc_a = Atom('tscell')
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
            [bd0, 0, 1.2, ts0, True, None],
            [bd1, 3, 4.5, ts1, False, None]
        ]
        self.test_key = ['hash1', 'user2', ts0]
        self.table = Table(None, table_name)

    def test_encode_data_for_get(self):
        keylist = [
            (tsc_a, str_to_bytes('hash1'), udef_a, udef_a, udef_a, udef_a),
            (tsc_a, str_to_bytes('user2'), udef_a, udef_a, udef_a, udef_a),
            (tsc_a, udef_a, udef_a, unix_time_millis(ts0), udef_a, udef_a)
        ]
        req = Atom('tsgetreq'), str_to_bytes(table_name), keylist, udef_a
        req_test = encode(req)

        req_encoded = self.c._encode_timeseries_keyreq(self.table, self.test_key)
        self.assertEqual(req_test, req_encoded)

    def test_encode_data_for_put(self):
        r0 = [
            (tsc_a, bd0, udef_a, udef_a, udef_a, udef_a),
            (tsc_a, udef_a, 0, udef_a, udef_a, udef_a),
            (tsc_a, udef_a, udef_a, udef_a, udef_a, 1.2),
            (tsc_a, udef_a, udef_a, unix_time_millis(ts0), udef_a, udef_a),
            (tsc_a, udef_a, udef_a, udef_a, True, udef_a),
            (tsc_a, udef_a, udef_a, udef_a, udef_a, udef_a)
        ]
        r1 = [
            (tsc_a, bd1, udef_a, udef_a, udef_a, udef_a),
            (tsc_a, udef_a, 3, udef_a, udef_a, udef_a),
            (tsc_a, udef_a, udef_a, udef_a, udef_a, 4.5),
            (tsc_a, udef_a, udef_a, unix_time_millis(ts1), udef_a, udef_a),
            (tsc_a, udef_a, udef_a, udef_a, False, udef_a),
            (tsc_a, udef_a, udef_a, udef_a, udef_a, udef_a)
        ]
        rows = [r0, r1]
        req = Atom('tsputreq'), str_to_bytes(table_name), udef_a, rows
        req_test = encode(req)

        tsobj = TsObject(None, self.table, self.rows, None)
        req_encoded = self.c._encode_timeseries_put(tsobj)
        self.assertEqual(req_test, req_encoded)
