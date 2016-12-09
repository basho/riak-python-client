# -*- coding: utf-8 -*-
import datetime
import six
import unittest

import riak.pb.riak_ts_pb2
from riak.pb.riak_ts_pb2 import TsColumnType

from riak import RiakError
from riak.codecs.pbuf import PbufCodec
from riak.table import Table
from riak.tests import RUN_TIMESERIES
from riak.tests.base import IntegrationTestBase
from riak.ts_object import TsObject
from riak.util import str_to_bytes, bytes_to_str, \
    unix_time_millis, datetime_from_unix_time_millis, \
    is_timeseries_supported

table_name = 'GeoCheckin'

bd0 = '时间序列'
bd1 = 'временные ряды'

blob0 = b'\x00\x01\x02\x03\x04\x05\x06\x07'

fiveMins = datetime.timedelta(0, 300)
# NB: last arg is microseconds, 987ms expressed
ts0 = datetime.datetime(2015, 1, 1, 12, 0, 0, 987000)
ex0ms = 1420113600987

ts1 = ts0 + fiveMins
ex1ms = 1420113900987


@unittest.skipUnless(is_timeseries_supported(),
                     'Timeseries not supported by this Python version')
class TimeseriesPbufUnitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ts0ms = unix_time_millis(ts0)
        if cls.ts0ms != ex0ms:
            raise AssertionError(
                'expected {:d} to equal {:d}'.format(cls.ts0ms, ex0ms))

        cls.ts1ms = unix_time_millis(ts1)
        if cls.ts1ms != ex1ms:
            raise AssertionError(
                'expected {:d} to equal {:d}'.format(cls.ts1ms, ex1ms))

        cls.rows = [
            [bd0, 0, 1.2, ts0, True, None],
            [bd1, 3, 4.5, ts1, False, blob0]
        ]
        cls.test_key = ['hash1', 'user2', ts0]
        cls.table = Table(None, table_name)

    def validate_keyreq(self, req):
        self.assertEqual(self.table.name, bytes_to_str(req.table))
        self.assertEqual(len(self.test_key), len(req.key))
        self.assertEqual('hash1', bytes_to_str(req.key[0].varchar_value))
        self.assertEqual('user2', bytes_to_str(req.key[1].varchar_value))
        self.assertEqual(self.ts0ms, req.key[2].timestamp_value)

    def test_encode_decode_timestamp(self):
        ts0ms = unix_time_millis(ts0)
        self.assertEqual(ts0ms, ex0ms)
        ts0_d = datetime_from_unix_time_millis(ts0ms)
        self.assertEqual(ts0, ts0_d)

    def test_encode_data_for_get(self):
        c = PbufCodec()
        msg = c.encode_timeseries_keyreq(
                self.table, self.test_key, is_delete=False)
        req = riak.pb.riak_ts_pb2.TsGetReq()
        req.ParseFromString(msg.data)
        self.validate_keyreq(req)

    def test_encode_data_for_delete(self):
        c = PbufCodec()
        msg = c.encode_timeseries_keyreq(
                self.table, self.test_key, is_delete=True)
        req = riak.pb.riak_ts_pb2.TsDelReq()
        req.ParseFromString(msg.data)
        self.validate_keyreq(req)

    def test_encode_data_for_put(self):
        c = PbufCodec()
        tsobj = TsObject(None, self.table, self.rows, None)
        msg = c.encode_timeseries_put(tsobj)
        req = riak.pb.riak_ts_pb2.TsPutReq()
        req.ParseFromString(msg.data)

        # NB: expected, actual
        self.assertEqual(self.table.name, bytes_to_str(req.table))
        self.assertEqual(len(self.rows), len(req.rows))

        r0 = req.rows[0]
        self.assertEqual(bytes_to_str(r0.cells[0].varchar_value),
                         self.rows[0][0])
        self.assertEqual(r0.cells[1].sint64_value, self.rows[0][1])
        self.assertEqual(r0.cells[2].double_value, self.rows[0][2])
        self.assertEqual(r0.cells[3].timestamp_value, self.ts0ms)
        self.assertEqual(r0.cells[4].boolean_value, self.rows[0][4])
        self.assertFalse(r0.cells[5].HasField('varchar_value'))

        r1 = req.rows[1]
        self.assertEqual(bytes_to_str(r1.cells[0].varchar_value),
                         self.rows[1][0])
        self.assertEqual(r1.cells[1].sint64_value, self.rows[1][1])
        self.assertEqual(r1.cells[2].double_value, self.rows[1][2])
        self.assertEqual(r1.cells[3].timestamp_value, self.ts1ms)
        self.assertEqual(r1.cells[4].boolean_value, self.rows[1][4])
        self.assertEqual(r1.cells[5].varchar_value, self.rows[1][5])

    def test_encode_data_for_listkeys(self):
        c = PbufCodec(client_timeouts=True)
        msg = c.encode_timeseries_listkeysreq(self.table, 1234)
        req = riak.pb.riak_ts_pb2.TsListKeysReq()
        req.ParseFromString(msg.data)
        self.assertEqual(self.table.name, bytes_to_str(req.table))
        self.assertEqual(1234, req.timeout)

    def test_decode_data_from_query(self):
        tqr = riak.pb.riak_ts_pb2.TsQueryResp()

        c0 = tqr.columns.add()
        c0.name = str_to_bytes('col_varchar')
        c0.type = TsColumnType.Value('VARCHAR')
        c1 = tqr.columns.add()
        c1.name = str_to_bytes('col_integer')
        c1.type = TsColumnType.Value('SINT64')
        c2 = tqr.columns.add()
        c2.name = str_to_bytes('col_double')
        c2.type = TsColumnType.Value('DOUBLE')
        c3 = tqr.columns.add()
        c3.name = str_to_bytes('col_timestamp')
        c3.type = TsColumnType.Value('TIMESTAMP')
        c4 = tqr.columns.add()
        c4.name = str_to_bytes('col_boolean')
        c4.type = TsColumnType.Value('BOOLEAN')
        c5 = tqr.columns.add()
        c5.name = str_to_bytes('col_blob')
        c5.type = TsColumnType.Value('BLOB')

        r0 = tqr.rows.add()
        r0c0 = r0.cells.add()
        r0c0.varchar_value = str_to_bytes(self.rows[0][0])
        r0c1 = r0.cells.add()
        r0c1.sint64_value = self.rows[0][1]
        r0c2 = r0.cells.add()
        r0c2.double_value = self.rows[0][2]
        r0c3 = r0.cells.add()
        r0c3.timestamp_value = self.ts0ms
        r0c4 = r0.cells.add()
        r0c4.boolean_value = self.rows[0][4]
        r0.cells.add()

        r1 = tqr.rows.add()
        r1c0 = r1.cells.add()
        r1c0.varchar_value = str_to_bytes(self.rows[1][0])
        r1c1 = r1.cells.add()
        r1c1.sint64_value = self.rows[1][1]
        r1c2 = r1.cells.add()
        r1c2.double_value = self.rows[1][2]
        r1c3 = r1.cells.add()
        r1c3.timestamp_value = self.ts1ms
        r1c4 = r1.cells.add()
        r1c4.boolean_value = self.rows[1][4]
        r1c5 = r1.cells.add()
        r1c5.varchar_value = self.rows[1][5]

        tsobj = TsObject(None, self.table)
        c = PbufCodec()
        c.decode_timeseries(tqr, tsobj, True)

        self.assertEqual(len(tsobj.rows), len(self.rows))
        self.assertEqual(len(tsobj.columns.names), len(tqr.columns))
        self.assertEqual(len(tsobj.columns.types), len(tqr.columns))

        cn, ct = tsobj.columns
        self.assertEqual(cn[0], 'col_varchar')
        self.assertEqual(ct[0], 'varchar')
        self.assertEqual(cn[1], 'col_integer')
        self.assertEqual(ct[1], 'sint64')
        self.assertEqual(cn[2], 'col_double')
        self.assertEqual(ct[2], 'double')
        self.assertEqual(cn[3], 'col_timestamp')
        self.assertEqual(ct[3], 'timestamp')
        self.assertEqual(cn[4], 'col_boolean')
        self.assertEqual(ct[4], 'boolean')
        self.assertEqual(cn[5], 'col_blob')
        self.assertEqual(ct[5], 'blob')

        r0 = tsobj.rows[0]
        self.assertEqual(bytes_to_str(r0[0]), self.rows[0][0])
        self.assertEqual(r0[1], self.rows[0][1])
        self.assertEqual(r0[2], self.rows[0][2])
        self.assertEqual(r0[3], ts0)
        self.assertEqual(r0[4], self.rows[0][4])
        self.assertEqual(r0[5], self.rows[0][5])

        r1 = tsobj.rows[1]
        self.assertEqual(bytes_to_str(r1[0]), self.rows[1][0])
        self.assertEqual(r1[1], self.rows[1][1])
        self.assertEqual(r1[2], self.rows[1][2])
        self.assertEqual(r1[3], ts1)
        self.assertEqual(r1[4], self.rows[1][4])
        self.assertEqual(r1[5], self.rows[1][5])


@unittest.skipUnless(is_timeseries_supported() and RUN_TIMESERIES,
                     'Timeseries not supported by this Python version'
                     ' or RUN_TIMESERIES is 0')
class TimeseriesPbufTests(IntegrationTestBase, unittest.TestCase):
    client_options = {'transport_options':
                      {'use_ttb': False, 'ts_convert_timestamp': True}}

    @classmethod
    def setUpClass(cls):
        super(TimeseriesPbufTests, cls).setUpClass()
        cls.now = datetime.datetime.utcfromtimestamp(144379690.987000)
        fiveMinsAgo = cls.now - fiveMins
        tenMinsAgo = fiveMinsAgo - fiveMins
        fifteenMinsAgo = tenMinsAgo - fiveMins
        twentyMinsAgo = fifteenMinsAgo - fiveMins
        twentyFiveMinsAgo = twentyMinsAgo - fiveMins

        client = cls.create_client()
        table = client.table(table_name)
        rows = [
            ['hash1', 'user2', twentyFiveMinsAgo, 'typhoon', 90.3],
            ['hash1', 'user2', twentyMinsAgo, 'hurricane', 82.3],
            ['hash1', 'user2', fifteenMinsAgo, 'rain', 79.0],
            ['hash1', 'user2', fiveMinsAgo, 'wind', None],
            ['hash1', 'user2', cls.now, 'snow', 20.1]
        ]
        try:
            ts_obj = table.new(rows)
            result = ts_obj.store()
        except (RiakError, NotImplementedError) as e:
            raise unittest.SkipTest(e)
        finally:
            client.close()
        if result is not True:
            raise AssertionError("expected success")

        cls.nowMsec = unix_time_millis(cls.now)
        cls.fiveMinsAgo = fiveMinsAgo
        cls.twentyMinsAgo = twentyMinsAgo
        cls.twentyFiveMinsAgo = twentyFiveMinsAgo
        cls.tenMinsAgoMsec = unix_time_millis(tenMinsAgo)
        cls.twentyMinsAgoMsec = unix_time_millis(twentyMinsAgo)
        cls.numCols = len(rows[0])
        cls.rows = rows
        encoded_rows = [
            [str_to_bytes('hash1'), str_to_bytes('user2'),
             twentyFiveMinsAgo, str_to_bytes('typhoon'), 90.3],
            [str_to_bytes('hash1'), str_to_bytes('user2'),
             twentyMinsAgo, str_to_bytes('hurricane'), 82.3],
            [str_to_bytes('hash1'), str_to_bytes('user2'),
             fifteenMinsAgo, str_to_bytes('rain'), 79.0],
            [str_to_bytes('hash1'), str_to_bytes('user2'),
             fiveMinsAgo, str_to_bytes('wind'), None],
            [str_to_bytes('hash1'), str_to_bytes('user2'),
             cls.now, str_to_bytes('snow'), 20.1]
        ]
        cls.encoded_rows = encoded_rows

    def validate_len(self, ts_obj, elen):
        if isinstance(elen, tuple):
            self.assertIn(len(ts_obj.columns.names), elen)
            self.assertIn(len(ts_obj.columns.types), elen)
            self.assertIn(len(ts_obj.rows), elen)
        else:
            self.assertEqual(len(ts_obj.columns.names), elen)
            self.assertEqual(len(ts_obj.columns.types), elen)
            self.assertEqual(len(ts_obj.rows), elen)

    def validate_data(self, ts_obj):
        if ts_obj.columns is not None:
            self.assertEqual(len(ts_obj.columns.names), self.numCols)
            self.assertEqual(len(ts_obj.columns.types), self.numCols)
        self.assertEqual(len(ts_obj.rows), 1)
        row = ts_obj.rows[0]
        self.assertEqual(bytes_to_str(row[0]), 'hash1')
        self.assertEqual(bytes_to_str(row[1]), 'user2')
        self.assertEqual(row[2], self.fiveMinsAgo)
        self.assertEqual(row[2].microsecond, 987000)
        self.assertEqual(bytes_to_str(row[3]), 'wind')
        self.assertIsNone(row[4])

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
        self.validate_len(ts_obj, 0)

    def test_query_that_returns_table_description(self):
        fmt = 'DESCRIBE {table}'
        query = fmt.format(table=table_name)
        ts_obj = self.client.ts_query(table_name, query)
        self.assertIsNotNone(ts_obj)
        self.validate_len(ts_obj, (5, 7, 8))

    def test_query_that_returns_table_description_using_interpolation(self):
        query = 'Describe {table}'
        ts_obj = self.client.ts_query(table_name, query)
        self.assertIsNotNone(ts_obj)
        self.validate_len(ts_obj, (5, 7, 8))

    def test_query_description_via_table(self):
        query = 'describe {table}'
        table = Table(self.client, table_name)
        ts_obj = table.query(query)
        self.assertIsNotNone(ts_obj)
        self.validate_len(ts_obj, (5, 7, 8))

    def test_get_description(self):
        ts_obj = self.client.ts_describe(table_name)
        self.assertIsNotNone(ts_obj)
        self.validate_len(ts_obj, (5, 7, 8))

    def test_get_description_via_table(self):
        table = Table(self.client, table_name)
        ts_obj = table.describe()
        self.assertIsNotNone(ts_obj)
        self.validate_len(ts_obj, (5, 7, 8))

    def test_query_that_returns_no_data(self):
        fmt = """
        select * from {table} where
            time > 0 and time < 10 and
            geohash = 'hash1' and
            user = 'user1'
        """
        query = fmt.format(table=table_name)
        ts_obj = self.client.ts_query(table_name, query)
        self.validate_len(ts_obj, 0)

    def test_query_that_returns_no_data_using_interpolation(self):
        query = """
        select * from {table} where
            time > 0 and time < 10 and
            geohash = 'hash1' and
            user = 'user1'
        """
        ts_obj = self.client.ts_query(table_name, query)
        self.validate_len(ts_obj, 0)

    def test_query_that_matches_some_data(self):
        fmt = """
        select * from {table} where
            time > {t1} and time < {t2} and
            geohash = 'hash1' and
            user = 'user2'
        """
        query = fmt.format(
                table=table_name,
                t1=self.tenMinsAgoMsec,
                t2=self.nowMsec)
        ts_obj = self.client.ts_query(table_name, query)
        self.validate_data(ts_obj)

    def test_query_that_matches_some_data_using_interpolation(self):
        fmt = """
        select * from {{table}} where
            time > {t1} and time < {t2} and
            geohash = 'hash1' and
            user = 'user2'
        """
        query = fmt.format(
                t1=self.tenMinsAgoMsec,
                t2=self.nowMsec)
        ts_obj = self.client.ts_query(table_name, query)
        self.validate_data(ts_obj)

    def test_query_that_matches_more_data(self):
        fmt = """
        select * from {table} where
            time >= {t1} and time <= {t2} and
            geohash = 'hash1' and
            user = 'user2'
        """
        query = fmt.format(
                table=table_name,
                t1=self.twentyMinsAgoMsec,
                t2=self.nowMsec)
        ts_obj = self.client.ts_query(table_name, query)
        j = 0
        for i, want in enumerate(self.encoded_rows):
            if want[2] == self.twentyFiveMinsAgo:
                continue
            got = ts_obj.rows[j]
            j += 1
            self.assertListEqual(got, want)

    def test_get_with_invalid_key(self):
        key = ['hash1', 'user2']
        with self.assertRaises(RiakError):
            self.client.ts_get(table_name, key)

    def test_get_single_value(self):
        key = ['hash1', 'user2', self.fiveMinsAgo]
        ts_obj = self.client.ts_get(table_name, key)
        self.assertIsNotNone(ts_obj)
        self.validate_data(ts_obj)

    def test_get_single_value_via_table(self):
        key = ['hash1', 'user2', self.fiveMinsAgo]
        table = Table(self.client, table_name)
        ts_obj = table.get(key)
        self.assertIsNotNone(ts_obj)
        self.validate_data(ts_obj)

    def test_stream_keys(self):
        table = Table(self.client, table_name)
        streamed_keys = []
        for keylist in table.stream_keys():
            self.assertNotEqual([], keylist)
            streamed_keys += keylist
            for key in keylist:
                self.assertIsInstance(key, list)
                self.assertEqual(len(key), 3)
                self.assertEqual(bytes_to_str(key[0]), 'hash1')
                self.assertEqual(bytes_to_str(key[1]), 'user2')
                self.assertIsInstance(key[2], datetime.datetime)
        self.assertGreater(len(streamed_keys), 0)

    def test_delete_single_value(self):
        key = ['hash1', 'user2', self.twentyFiveMinsAgo]
        rslt = self.client.ts_delete(table_name, key)
        self.assertTrue(rslt)
        ts_obj = self.client.ts_get(table_name, key)
        self.assertIsNotNone(ts_obj)
        self.assertEqual(len(ts_obj.rows), 0)
        self.assertEqual(len(ts_obj.columns.names), 0)
        self.assertEqual(len(ts_obj.columns.types), 0)

    def test_create_error_via_put(self):
        table = Table(self.client, table_name)
        ts_obj = table.new([])
        with self.assertRaises(RiakError):
            ts_obj.store()

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
