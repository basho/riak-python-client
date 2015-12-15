# -*- coding: utf-8 -*-
import datetime
import platform
import riak.pb.riak_ts_pb2

from riak import RiakError
from riak.table import Table
from riak.ts_object import TsObject
from riak.transports.pbc.codec import RiakPbcCodec
from riak.util import str_to_bytes, bytes_to_str
from riak.tests import RUN_TIMESERIES
from riak.tests.base import IntegrationTestBase
from riak.pb.riak_ts_pb2 import TsColumnType

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


@unittest.skipUnless(RUN_TIMESERIES, 'RUN_TIMESERIES is 0')
class TimeseriesUnitTests(unittest.TestCase):
    def setUp(self):
        self.c = RiakPbcCodec()
        self.ts0ms = self.c._unix_time_millis(ts0)
        self.ts1ms = self.c._unix_time_millis(ts1)
        self.rows = [
            [bd0, 0, 1.2, ts0, True],
            [bd1, 3, 4.5, ts1, False]
        ]
        self.test_key = ['hash1', 'user2', ts0]
        self.table = Table(None, 'test-table')

    def validate_keyreq(self, req):
        self.assertEqual(self.table.name, bytes_to_str(req.table))
        self.assertEqual(len(self.test_key), len(req.key))
        self.assertEqual('hash1', bytes_to_str(req.key[0].varchar_value))
        self.assertEqual('user2', bytes_to_str(req.key[1].varchar_value))
        self.assertEqual(self.ts0ms, req.key[2].timestamp_value)

    def test_encode_data_for_get(self):
        req = riak.pb.riak_ts_pb2.TsGetReq()
        self.c._encode_timeseries_keyreq(self.table, self.test_key, req)
        self.validate_keyreq(req)

    def test_encode_data_for_delete(self):
        req = riak.pb.riak_ts_pb2.TsDelReq()
        self.c._encode_timeseries_keyreq(self.table, self.test_key, req)
        self.validate_keyreq(req)

    def test_encode_data_for_put(self):
        tsobj = TsObject(None, self.table, self.rows, None)
        ts_put_req = riak.pb.riak_ts_pb2.TsPutReq()
        self.c._encode_timeseries_put(tsobj, ts_put_req)

        # NB: expected, actual
        self.assertEqual(self.table.name, bytes_to_str(ts_put_req.table))
        self.assertEqual(len(self.rows), len(ts_put_req.rows))

        r0 = ts_put_req.rows[0]
        self.assertEqual(bytes_to_str(r0.cells[0].varchar_value),
                         self.rows[0][0])
        self.assertEqual(r0.cells[1].sint64_value, self.rows[0][1])
        self.assertEqual(r0.cells[2].double_value, self.rows[0][2])
        self.assertEqual(r0.cells[3].timestamp_value, self.ts0ms)
        self.assertEqual(r0.cells[4].boolean_value, self.rows[0][4])

        r1 = ts_put_req.rows[1]
        self.assertEqual(bytes_to_str(r1.cells[0].varchar_value),
                         self.rows[1][0])
        self.assertEqual(r1.cells[1].sint64_value, self.rows[1][1])
        self.assertEqual(r1.cells[2].double_value, self.rows[1][2])
        self.assertEqual(r1.cells[3].timestamp_value, self.ts1ms)
        self.assertEqual(r1.cells[4].boolean_value, self.rows[1][4])

    def test_encode_data_for_listkeys(self):
        req = riak.pb.riak_ts_pb2.TsListKeysReq()
        self.c._encode_timeseries_listkeysreq(self.table, req, 1234)
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

        tsobj = TsObject(None, self.table, [], [])
        c = RiakPbcCodec()
        c._decode_timeseries(tqr, tsobj)

        self.assertEqual(len(self.rows), len(tsobj.rows))
        self.assertEqual(len(tqr.columns), len(tsobj.columns))

        c = tsobj.columns
        self.assertEqual(c[0][0], 'col_varchar')
        self.assertEqual(c[0][1], TsColumnType.Value('VARCHAR'))
        self.assertEqual(c[1][0], 'col_integer')
        self.assertEqual(c[1][1], TsColumnType.Value('SINT64'))
        self.assertEqual(c[2][0], 'col_double')
        self.assertEqual(c[2][1], TsColumnType.Value('DOUBLE'))
        self.assertEqual(c[3][0], 'col_timestamp')
        self.assertEqual(c[3][1], TsColumnType.Value('TIMESTAMP'))
        self.assertEqual(c[4][0], 'col_boolean')
        self.assertEqual(c[4][1], TsColumnType.Value('BOOLEAN'))

        r0 = tsobj.rows[0]
        self.assertEqual(r0[0], self.rows[0][0])
        self.assertEqual(r0[1], self.rows[0][1])
        self.assertEqual(r0[2], self.rows[0][2])
        self.assertEqual(r0[3], ts0)
        self.assertEqual(r0[4], self.rows[0][4])

        r1 = tsobj.rows[1]
        self.assertEqual(r1[0], self.rows[1][0])
        self.assertEqual(r1[1], self.rows[1][1])
        self.assertEqual(r1[2], self.rows[1][2])
        self.assertEqual(r1[3], ts1)
        self.assertEqual(r1[4], self.rows[1][4])


@unittest.skipUnless(RUN_TIMESERIES, 'RUN_TIMESERIES is 0')
class TimeseriesTests(IntegrationTestBase, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TimeseriesTests, cls).setUpClass()
        cls.now = datetime.datetime.utcfromtimestamp(144379690)
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
        ts_obj = table.new(rows)
        result = ts_obj.store()
        if not result:
            raise AssertionError("expected success")
        client.close()

        codec = RiakPbcCodec()
        cls.nowMsec = codec._unix_time_millis(cls.now)
        cls.fiveMinsAgo = fiveMinsAgo
        cls.twentyMinsAgo = twentyMinsAgo
        cls.twentyFiveMinsAgo = twentyFiveMinsAgo
        cls.tenMinsAgoMsec = codec._unix_time_millis(tenMinsAgo)
        cls.twentyMinsAgoMsec = codec._unix_time_millis(twentyMinsAgo)
        cls.numCols = len(rows[0])
        cls.rows = rows

    def validate_data(self, ts_obj):
        if ts_obj.columns is not None:
            self.assertEqual(len(ts_obj.columns), self.numCols)
        self.assertEqual(len(ts_obj.rows), 1)
        row = ts_obj.rows[0]
        self.assertEqual(row[0], 'hash1')
        self.assertEqual(row[1], 'user2')
        self.assertEqual(row[2], self.fiveMinsAgo)
        self.assertEqual(row[3], 'wind')
        self.assertIsNone(row[4])

    def test_query_that_returns_no_data(self):
        fmt = """
        select * from {table} where
            time > 0 and time < 10 and
            geohash = 'hash1' and
            user = 'user1'
        """
        query = fmt.format(table=table_name)
        ts_obj = self.client.ts_query('GeoCheckin', query)
        self.assertEqual(len(ts_obj.columns), 0)
        self.assertEqual(len(ts_obj.rows), 0)

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
        ts_obj = self.client.ts_query('GeoCheckin', query)
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
        ts_obj = self.client.ts_query('GeoCheckin', query)
        j = 0
        for i, want in enumerate(self.rows):
            if want[2] == self.twentyFiveMinsAgo:
                continue
            got = ts_obj.rows[j]
            j += 1
            self.assertListEqual(got, want)

    def test_get_with_invalid_key(self):
        key = ['hash1', 'user2']
        with self.assertRaises(RiakError):
            self.client.ts_get('GeoCheckin', key)

    def test_get_single_value(self):
        key = ['hash1', 'user2', self.fiveMinsAgo]
        ts_obj = self.client.ts_get('GeoCheckin', key)
        self.assertIsNotNone(ts_obj)
        self.validate_data(ts_obj)

    def test_get_single_value_via_table(self):
        key = ['hash1', 'user2', self.fiveMinsAgo]
        table = Table(self.client, 'GeoCheckin')
        ts_obj = table.get(key)
        self.assertIsNotNone(ts_obj)
        self.validate_data(ts_obj)

    def test_stream_keys(self):
        table = Table(self.client, 'GeoCheckin')
        streamed_keys = []
        for keylist in table.stream_keys():
            self.assertNotEqual([], keylist)
            streamed_keys += keylist
            for key in keylist:
                self.assertIsInstance(key, list)
                self.assertEqual(len(key), 3)
                self.assertEqual('hash1', key[0])
                self.assertEqual('user2', key[1])
                self.assertIsInstance(key[2], datetime.datetime)
        self.assertGreater(len(streamed_keys), 0)

    def test_delete_single_value(self):
        key = ['hash1', 'user2', self.twentyFiveMinsAgo]
        rslt = self.client.ts_delete('GeoCheckin', key)
        self.assertTrue(rslt)
        ts_obj = self.client.ts_get('GeoCheckin', key)
        self.assertEqual(len(ts_obj.rows), 0)
