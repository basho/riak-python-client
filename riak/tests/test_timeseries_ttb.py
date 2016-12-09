# -*- coding: utf-8 -*-
import datetime
import logging
import six
import unittest

from erlastic import decode, encode
from erlastic.types import Atom

from riak import RiakError
from riak.table import Table
from riak.tests import RUN_TIMESERIES
from riak.ts_object import TsObject
from riak.codecs.ttb import TtbCodec
from riak.util import str_to_bytes, bytes_to_str, \
    unix_time_millis, is_timeseries_supported
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

blob0 = b'\x00\x01\x02\x03\x04\x05\x06\x07'

fiveMins = datetime.timedelta(0, 300)
ts0 = datetime.datetime(2015, 1, 1, 12, 1, 2, 987000)
ts1 = ts0 + fiveMins


@unittest.skipUnless(is_timeseries_supported(),
                     'Timeseries not supported by this Python version')
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
    #      <<"weather">>, <<"temperature">>, <<"blob">>],
    #     [varchar, varchar, timestamp, varchar, double, blob],
    #     [(<<"hash1">>, <<"user2">>, 144378190987, <<"typhoon">>,
    #         90.3, <<0,1,2,3,4,5,6,7>>)]
    #   }
    # }
    def test_decode_data_from_get(self):
        colnames = ["varchar", "sint64", "double", "timestamp",
                    "boolean", "varchar", "varchar", "blob"]
        coltypes = [varchar_a, sint64_a, double_a, timestamp_a,
                    boolean_a, varchar_a, varchar_a]
        r0 = (bd0, 0, 1.2, unix_time_millis(ts0), True,
              [], str1, None, None)
        r1 = (bd1, 3, 4.5, unix_time_millis(ts1), False,
              [], str1, None, blob0)
        rows = [r0, r1]
        # { tsgetresp, { [colnames], [coltypes], [rows] } }
        data_t = colnames, coltypes, rows
        rsp_data = tsgetresp_a, data_t
        rsp_ttb = encode(rsp_data)

        tsobj = TsObject(None, self.table)
        c = TtbCodec()
        c.decode_timeseries(decode(rsp_ttb), tsobj)

        for i in range(0, 1):
            dr = rows[i]
            r = tsobj.rows[i]  # encoded
            self.assertEqual(r[0], dr[0].encode('utf-8'))
            self.assertEqual(r[1], dr[1])
            self.assertEqual(r[2], dr[2])
            # NB *not* decoding timestamps
            # dt = datetime_from_unix_time_millis(dr[3])
            self.assertEqual(r[3], dr[3])
            if i == 0:
                self.assertEqual(r[4], True)
            else:
                self.assertEqual(r[4], False)
            self.assertEqual(r[5], None)
            self.assertEqual(r[6], dr[6].encode('ascii'))
            self.assertEqual(r[7], None)
            self.assertEqual(r[8], dr[8])

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
                     'Timeseries not supported by this Python version'
                     ' or RUN_TIMESERIES is 0')
class TimeseriesTtbTests(IntegrationTestBase, unittest.TestCase):
    client_options = {'transport_options':
                      {'use_ttb': True, 'ts_convert_timestamp': True}}

    @classmethod
    def setUpClass(cls):
        super(TimeseriesTtbTests, cls).setUpClass()
        client = cls.create_client()
        skey = 'test-key'
        btype = client.bucket_type(table_name)
        bucket = btype.bucket(table_name)
        try:
            bucket.get(skey)
        except (RiakError, NotImplementedError) as e:
            raise unittest.SkipTest(e)
        finally:
            client.close()

    def validate_len(self, ts_obj, elen):
        if isinstance(elen, tuple):
            self.assertIn(len(ts_obj.columns.names), elen)
            self.assertIn(len(ts_obj.columns.types), elen)
            self.assertIn(len(ts_obj.rows), elen)
        else:
            self.assertEqual(len(ts_obj.columns.names), elen)
            self.assertEqual(len(ts_obj.columns.types), elen)
            self.assertEqual(len(ts_obj.rows), elen)

    def test_insert_data_via_sql(self):
        query = """
            INSERT INTO GeoCheckin_Wide
            (geohash, user, time, weather, temperature, uv_index, observed)
                VALUES
            ('hash3', 'user3', 1460203200000, 'tornado', 43.5, 128, True);
        """
        ts_obj = self.client.ts_query('GeoCheckin_Wide', query)
        self.assertIsNotNone(ts_obj)
        self.validate_len(ts_obj, 0)

    def test_query_that_creates_table_using_interpolation(self):
        table = self.randname()
        query = """CREATE TABLE test-{table} (
            geohash varchar not null,
            user varchar not null,
            time timestamp not null,
            weather varchar not null,
            temperature double,
            PRIMARY KEY((geohash, user, quantum(time, 15, m)),
                geohash, user, time))
        """
        ts_obj = self.client.ts_query(table, query)
        self.assertIsNotNone(ts_obj)
        self.assertFalse(hasattr(ts_obj, 'ts_cols'))
        self.assertIsNone(ts_obj.rows)

    def test_query_that_returns_table_description(self):
        fmt = 'DESCRIBE {table}'
        query = fmt.format(table=table_name)
        ts_obj = self.client.ts_query(table_name, query)
        self.assertIsNotNone(ts_obj)
        self.validate_len(ts_obj, (5, 7, 8))

    def test_store_and_fetch_gh_483(self):
        now = datetime.datetime(2015, 1, 1, 12, 0, 0)
        table = self.client.table(table_name)
        rows = [
            ['hash1', 'user2', now, 'frazzle', 12.3]
        ]

        ts_obj = table.new(rows)
        result = ts_obj.store()
        self.assertTrue(result)

        k = ['hash1', 'user2', now]
        ts_obj = self.client.ts_get(table_name, k)
        self.assertIsNotNone(ts_obj)
        ts_cols = ts_obj.columns
        self.assertEqual(len(ts_cols.names), 5)
        self.assertEqual(len(ts_cols.types), 5)
        self.assertEqual(len(ts_obj.rows), 1)

        row = ts_obj.rows[0]
        self.assertEqual(len(row), 5)
        exp = [six.b('hash1'), six.b('user2'), now,
               six.b('frazzle'), 12.3]
        self.assertEqual(row, exp)

    def test_store_and_fetch_and_query(self):
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
        # NB: response data is binary
        exp_rows = [
            [six.b('hash1'), six.b('user2'), twentyFiveMinsAgo,
                six.b('typhoon'), 90.3],
            [six.b('hash1'), six.b('user2'), twentyMinsAgo,
                six.b('hurricane'), 82.3],
            [six.b('hash1'), six.b('user2'), fifteenMinsAgo,
                six.b('rain'), 79.0],
            [six.b('hash1'), six.b('user2'), fiveMinsAgo,
                six.b('wind'), None],
            [six.b('hash1'), six.b('user2'), now,
                six.b('snow'), 20.1]
        ]
        ts_obj = table.new(rows)
        result = ts_obj.store()
        self.assertTrue(result)

        for i, r in enumerate(rows):
            k = r[0:3]
            ts_obj = self.client.ts_get(table_name, k)
            self.assertIsNotNone(ts_obj)
            ts_cols = ts_obj.columns
            self.assertEqual(len(ts_cols.names), 5)
            self.assertEqual(len(ts_cols.types), 5)
            self.assertEqual(len(ts_obj.rows), 1)
            row = ts_obj.rows[0]
            exp = exp_rows[i]
            self.assertEqual(len(row), 5)
            self.assertEqual(row, exp)

        fmt = """
        select * from {table} where
            time > {t1} and time < {t2} and
            geohash = 'hash1' and
            user = 'user2'
        """
        query = fmt.format(
                table=table_name,
                t1=unix_time_millis(tenMinsAgo),
                t2=unix_time_millis(now))
        ts_obj = self.client.ts_query(table_name, query)
        if ts_obj.columns is not None:
            self.assertEqual(len(ts_obj.columns.names), 5)
            self.assertEqual(len(ts_obj.columns.types), 5)
        self.assertEqual(len(ts_obj.rows), 1)
        row = ts_obj.rows[0]
        self.assertEqual(bytes_to_str(row[0]), 'hash1')
        self.assertEqual(bytes_to_str(row[1]), 'user2')
        self.assertEqual(row[2], fiveMinsAgo)
        self.assertEqual(row[2].microsecond, 987000)
        self.assertEqual(bytes_to_str(row[3]), 'wind')
        self.assertIsNone(row[4])

    def test_create_error_via_put(self):
        table = Table(self.client, table_name)
        ts_obj = table.new([])
        with self.assertRaises(RiakError) as cm:
            ts_obj.store()
        logging.debug(
                "[test_timeseries_ttb] saw exception: {}"
                .format(cm.exception))
