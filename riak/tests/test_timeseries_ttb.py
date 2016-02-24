# -*- coding: utf-8 -*-
import datetime
import platform
import random
import string

from erlastic import decode, encode
from erlastic.types import Atom

from riak.client import RiakClient
from riak.table import Table
from riak.ts_object import TsObject
from riak.transports.ttb.codec import RiakTtbCodec
from riak.util import str_to_bytes, \
    unix_time_millis, datetime_from_unix_time_millis, \
    is_timeseries_supported
from riak.tests import RUN_TIMESERIES
from riak.tests.base import IntegrationTestBase

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest

rpberrorresp_a = Atom('rpberrorresp')
tsgetreq_a = Atom('tsgetreq')
tsgetresp_a = Atom('tsgetresp')
tsputreq_a = Atom('tsputreq')

udef_a = Atom('undefined')
tsc_a = Atom('tscell')
table_name = 'GeoCheckin'

bd0 = '时间序列'
bd1 = 'временные ряды'

fiveMins = datetime.timedelta(0, 300)
ts0 = datetime.datetime(2015, 1, 1, 12, 0, 0)
ts1 = ts0 + fiveMins


@unittest.skipUnless(is_timeseries_supported(), "Timeseries not supported")
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
        req = tsgetreq_a, str_to_bytes(table_name), keylist, udef_a
        req_test = encode(req)

        req_encoded = self.c._encode_timeseries_keyreq_ttb(self.table, self.test_key)
        self.assertEqual(req_test, req_encoded)

    # def test_decode_riak_error(self):

    def test_decode_data_from_get(self):
        cols = []
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
        # { tsgetresp, [cols], [rows] }
        rsp_data = tsgetresp_a, cols, rows # NB: Python tuple notation
        rsp_ttb = encode(rsp_data)

        tsobj = TsObject(None, self.table, [], [])
        self.c._decode_timeseries_ttb(decode(rsp_ttb), tsobj)

        for i in range(0, 1):
            dr = rows[i]
            r = tsobj.rows[i]
            self.assertEqual(r[0], dr[0][1])
            self.assertEqual(r[1], dr[1][2])
            self.assertEqual(r[2], dr[2][5])
            self.assertEqual(r[3],
                datetime_from_unix_time_millis(dr[3][3]))
            if i == 0:
                self.assertEqual(r[4], True)
            else:
                self.assertEqual(r[4], False)
            self.assertEqual(r[5], None)

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
        req = tsputreq_a, str_to_bytes(table_name), udef_a, rows
        req_test = encode(req)

        tsobj = TsObject(None, self.table, self.rows, None)
        req_encoded = self.c._encode_timeseries_put_ttb(tsobj)
        self.assertEqual(req_test, req_encoded)


@unittest.skipUnless(is_timeseries_supported() and RUN_TIMESERIES,
                     'Timeseries not supported or RUN_TIMESERIES is 0')
class TimeseriesTtbTests(IntegrationTestBase, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TimeseriesTtbTests, cls).setUpClass()

    def test_store_data_ttb(self):
        now = datetime.datetime.utcfromtimestamp(144379690.987000)
        fiveMinsAgo = now - fiveMins
        tenMinsAgo = fiveMinsAgo - fiveMins
        fifteenMinsAgo = tenMinsAgo - fiveMins
        twentyMinsAgo = fifteenMinsAgo - fiveMins
        twentyFiveMinsAgo = twentyMinsAgo - fiveMins

        client = RiakClient(protocol='pbc',
                          host='riak-test',
                          pb_port=10017,
                          transport_options={'use_ttb': True})

        table = client.table(table_name)
        rows = [
            ['hash1', 'user2', twentyFiveMinsAgo, 'typhoon', 90.3],
            ['hash1', 'user2', twentyMinsAgo, 'hurricane', 82.3],
            ['hash1', 'user2', fifteenMinsAgo, 'rain', 79.0],
            ['hash1', 'user2', fiveMinsAgo, 'wind', None],
            ['hash1', 'user2', now, 'snow', 20.1]
        ]
        ts_obj = table.new(rows)
        result = ts_obj.store()
        self.assertTrue(result)
        client.close()
