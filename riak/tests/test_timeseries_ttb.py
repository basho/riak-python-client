# -*- coding: utf-8 -*-
import datetime
import logging
import six
import unittest

from erlastic import decode, encode
from erlastic.types import Atom

from riak import RiakError
from riak.table import Table
from riak.ts_object import TsObject
from riak.codecs.ttb import TtbCodec
from riak.util import str_to_bytes, \
    unix_time_millis, datetime_from_unix_time_millis, \
    is_timeseries_supported
from riak.tests import RUN_TIMESERIES
from riak.tests.base import IntegrationTestBase

rpberrorresp_a = Atom('rpberrorresp')
tsgetreq_a = Atom('tsgetreq')
tsgetresp_a = Atom('tsgetresp')
tsputreq_a = Atom('tsputreq')

udef_a = Atom('undefined')
varchar_a = Atom('varchar')
sint64_a = Atom('sint64')
double_a = Atom('double')
timestamp_a = Atom('timestamp')
boolean_a = Atom('boolean')

table_name = 'GeoCheckin'

str0 = 'ascii-0'
str1 = 'ascii-1'

bd0 = six.u('时间序列')
bd1 = six.u('временные ряды')

fiveMins = datetime.timedelta(0, 300)
ts0 = datetime.datetime(2015, 1, 1, 12, 0, 0)
ts1 = ts0 + fiveMins


@unittest.skipUnless(is_timeseries_supported(), "Timeseries not supported")
class TimeseriesTtbUnitTests(unittest.TestCase):
    def setUp(self):
        self.table = Table(None, table_name)

    def test_encode_data_for_get(self):
        keylist = [
            str_to_bytes('hash1'), str_to_bytes('user2'), unix_time_millis(ts0)
        ]
        req = tsgetreq_a, str_to_bytes(table_name), keylist, udef_a
        req_test = encode(req)

        test_key = ['hash1', 'user2', ts0]
        c = TtbCodec()
        msg = c.encode_timeseries_keyreq(self.table, test_key)
        self.assertEqual(req_test, msg.data)

    # {tsgetresp,
    #   {
    #     [<<"geohash">>, <<"user">>, <<"time">>,
    #      <<"weather">>, <<"temperature">>],
    #     [varchar, varchar, timestamp, varchar, double]
    #   },
    #   [[<<"hash1">>, <<"user2">>, 144378190987, <<"typhoon">>, 90.3]]
    # }
    def test_decode_data_from_get(self):
        colnames = ["varchar", "sint64", "double", "timestamp",
                    "boolean", "varchar", "varchar"]
        coltypes = [varchar_a, sint64_a, double_a, timestamp_a,
                    boolean_a, varchar_a, varchar_a]
        r0 = (bd0, 0, 1.2, unix_time_millis(ts0), True,
              [], str1, None)
        r1 = (bd1, 3, 4.5, unix_time_millis(ts1), False,
              [], str1, None)
        rows = [r0, r1]
        # { tsgetresp, { [colnames], [coltypes] }, [rows] }
        cols_t = colnames, coltypes
        rsp_data = tsgetresp_a, cols_t, rows
        rsp_ttb = encode(rsp_data)

        tsobj = TsObject(None, self.table, [])
        c = TtbCodec()
        c.decode_timeseries(decode(rsp_ttb), tsobj)

        for i in range(0, 1):
            dr = rows[i]
            r = tsobj.rows[i]  # encoded
            self.assertEqual(r[0], dr[0].encode('utf-8'))
            self.assertEqual(r[1], dr[1])
            self.assertEqual(r[2], dr[2])
            dt = datetime_from_unix_time_millis(dr[3])
            self.assertEqual(r[3], dt)
            if i == 0:
                self.assertEqual(r[4], True)
            else:
                self.assertEqual(r[4], False)
            self.assertEqual(r[5], [])
            self.assertEqual(r[6], dr[6].encode('ascii'))
            self.assertEqual(r[7], None)

    def test_encode_data_for_put(self):
        r0 = (bd0, 0, 1.2, unix_time_millis(ts0), True, [])
        r1 = (bd1, 3, 4.5, unix_time_millis(ts1), False, [])
        rows = [r0, r1]
        req = tsputreq_a, str_to_bytes(table_name), [], rows
        req_test = encode(req)

        rows_to_encode = [
            [bd0, 0, 1.2, ts0, True, None],
            [bd1, 3, 4.5, ts1, False, None]
        ]

        tsobj = TsObject(None, self.table, rows_to_encode, None)
        c = TtbCodec()
        msg = c.encode_timeseries_put(tsobj)
        self.assertEqual(req_test, msg.data)


@unittest.skipUnless(is_timeseries_supported() and RUN_TIMESERIES,
                     'Timeseries not supported or RUN_TIMESERIES is 0')
class TimeseriesTtbTests(IntegrationTestBase, unittest.TestCase):
    client_options = {'transport_options': {'use_ttb': True}}

    @classmethod
    def setUpClass(cls):
        super(TimeseriesTtbTests, cls).setUpClass()

    def test_store_and_fetch_ttb(self):
        now = datetime.datetime.utcfromtimestamp(144379690.987000)
        fiveMinsAgo = now - fiveMins
        tenMinsAgo = fiveMinsAgo - fiveMins
        fifteenMinsAgo = tenMinsAgo - fiveMins
        twentyMinsAgo = fifteenMinsAgo - fiveMins
        twentyFiveMinsAgo = twentyMinsAgo - fiveMins

        table = self.client.table(table_name)
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

        for r in rows:
            k = r[0:3]
            ts_obj = self.client.ts_get(table_name, k)
            self.assertIsNotNone(ts_obj)
            self.assertEqual(len(ts_obj.rows), 1)
            self.assertEqual(len(ts_obj.rows[0]), 5)

    def test_create_error_via_put(self):
        table = Table(self.client, table_name)
        ts_obj = table.new([])
        with self.assertRaises(RiakError) as cm:
            ts_obj.store()
        logging.debug(
                "[test_timeseries_ttb] saw exception: {}"
                .format(cm.exception))
