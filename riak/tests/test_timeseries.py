# -*- coding: utf-8 -*-
import datetime
import os
import platform
import riak_pb
import sys
import time

from riak.table import Table
from riak.ts_object import TsObject
from riak.transports.pbc.codec import RiakPbcCodec
from riak import RiakClient
from riak.util import str_to_bytes
from riak.tests import SKIP_TIMESERIES, HOST, PROTOCOL, PB_PORT, HTTP_PORT, SECURITY_CREDS
from riak.tests.base import IntegrationTestBase

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest

table_name = 'GeoCheckin'

bd0 = os.urandom(16)
bd1 = os.urandom(16)

fiveMins = datetime.timedelta(0, 300)
ts0 = datetime.datetime(2015, 1, 1, 12, 0, 0)
ts1 = ts0 + fiveMins

s = [ 'foo', 'bar', 'baz' ]
m = {
    'foo': 'foo',
    'bar': 'bar',
    'baz': 'baz',
    'set': s
}
sj = ['"foo"', '"bar"', '"baz"']
mj = '{"baz": "baz", "set": ["foo", "bar", "baz"], "foo": "foo", "bar": "bar"}'

class TimeseriesUnitTests(unittest.TestCase):
    def setUp(self):
        self.c = RiakPbcCodec()
        self.ts0ms = self.c._unix_time_millis(ts0)
        self.ts1ms = self.c._unix_time_millis(ts1)
        self.rows = [
            [ bd0, 0, 1.2, ts0, True, s, m ],
            [ bd1, 3, 4.5, ts1, False, s, m ]
        ]
        self.table = Table(None, 'test-table')

    def test_encode_data(self):
        tsobj = TsObject(None, self.table, self.rows, None)
        ts_put_req = riak_pb.TsPutReq()
        self.c._encode_timeseries(tsobj, ts_put_req)

        # NB: expected, actual
        self.assertEqual(len(self.rows), len(ts_put_req.rows))

        r0 = ts_put_req.rows[0]
        self.assertEqual(r0.cells[0].binary_value, self.rows[0][0])
        self.assertEqual(r0.cells[1].integer_value, self.rows[0][1])
        self.assertEqual(r0.cells[2].double_value, self.rows[0][2])
        self.assertEqual(r0.cells[3].timestamp_value, self.ts0ms)
        self.assertEqual(r0.cells[4].boolean_value, self.rows[0][4])
        self.assertEqual(r0.cells[5].set_value, sj)
        self.assertEqual(r0.cells[6].map_value, mj)

        r1 = ts_put_req.rows[1]
        self.assertEqual(r1.cells[0].binary_value, self.rows[1][0])
        self.assertEqual(r1.cells[1].integer_value, self.rows[1][1])
        self.assertEqual(r1.cells[2].double_value, self.rows[1][2])
        self.assertEqual(r1.cells[3].timestamp_value, self.ts1ms)
        self.assertEqual(r1.cells[4].boolean_value, self.rows[1][4])
        self.assertEqual(r1.cells[5].set_value, sj)
        self.assertEqual(r1.cells[6].map_value, mj)

    def test_decode_data(self):
        tqr = riak_pb.TsQueryResp()

        c0 = tqr.columns.add()
        c0.name = str_to_bytes('col_binary')
        c0.type = riak_pb.TsColumnType.Value('BINARY')
        c1 = tqr.columns.add()
        c1.name = str_to_bytes('col_integer')
        c1.type = riak_pb.TsColumnType.Value('INTEGER')
        c2 = tqr.columns.add()
        c2.name = str_to_bytes('col_double')
        c2.type = riak_pb.TsColumnType.Value('FLOAT')
        c3 = tqr.columns.add()
        c3.name = str_to_bytes('col_timestamp')
        c3.type = riak_pb.TsColumnType.Value('TIMESTAMP')
        c4 = tqr.columns.add()
        c4.name = str_to_bytes('col_boolean')
        c4.type = riak_pb.TsColumnType.Value('BOOLEAN')
        c5 = tqr.columns.add()
        c5.name = str_to_bytes('col_set')
        c5.type = riak_pb.TsColumnType.Value('SET')
        c6 = tqr.columns.add()
        c6.name = str_to_bytes('col_map')
        c6.type = riak_pb.TsColumnType.Value('MAP')

        r0 = tqr.rows.add()
        r0c0 = r0.cells.add()
        r0c0.binary_value = self.rows[0][0]
        r0c1 = r0.cells.add()
        r0c1.integer_value = self.rows[0][1]
        r0c2 = r0.cells.add()
        r0c2.double_value = self.rows[0][2]
        r0c3 = r0.cells.add()
        r0c3.timestamp_value = self.ts0ms
        r0c4 = r0.cells.add()
        r0c4.boolean_value = self.rows[0][4]
        r0c5 = r0.cells.add()
        for j in sj:
            r0c5.set_value.append(j)
        r0c6 = r0.cells.add()
        r0c6.map_value = str_to_bytes(mj)

        r1 = tqr.rows.add()
        r1c0 = r1.cells.add()
        r1c0.binary_value = self.rows[1][0]
        r1c1 = r1.cells.add()
        r1c1.integer_value = self.rows[1][1]
        r1c2 = r1.cells.add()
        r1c2.double_value = self.rows[1][2]
        r1c3 = r1.cells.add()
        r1c3.timestamp_value = self.ts1ms
        r1c4 = r1.cells.add()
        r1c4.boolean_value = self.rows[1][4]
        r1c5 = r1.cells.add()
        for j in sj:
            r1c5.set_value.append(j)
        r1c6 = r1.cells.add()
        r1c6.map_value = str_to_bytes(mj)

        tsobj = TsObject(None, self.table, [], [])
        c = RiakPbcCodec()
        c._decode_timeseries(tqr, tsobj)

        self.assertEqual(len(self.rows), len(tsobj.rows))
        self.assertEqual(len(tqr.columns), len(tsobj.columns))

        c = tsobj.columns
        self.assertEqual(c[0][0], 'col_binary')
        self.assertEqual(c[0][1], riak_pb.TsColumnType.Value('BINARY'))
        self.assertEqual(c[1][0], 'col_integer')
        self.assertEqual(c[1][1], riak_pb.TsColumnType.Value('INTEGER'))
        self.assertEqual(c[2][0], 'col_double')
        self.assertEqual(c[2][1], riak_pb.TsColumnType.Value('FLOAT'))
        self.assertEqual(c[3][0], 'col_timestamp')
        self.assertEqual(c[3][1], riak_pb.TsColumnType.Value('TIMESTAMP'))
        self.assertEqual(c[4][0], 'col_boolean')
        self.assertEqual(c[4][1], riak_pb.TsColumnType.Value('BOOLEAN'))
        self.assertEqual(c[5][0], 'col_set')
        self.assertEqual(c[5][1], riak_pb.TsColumnType.Value('SET'))
        self.assertEqual(c[6][0], 'col_map')
        self.assertEqual(c[6][1], riak_pb.TsColumnType.Value('MAP'))

        r0 = tsobj.rows[0]
        self.assertEqual(r0[0], self.rows[0][0])
        self.assertEqual(r0[1], self.rows[0][1])
        self.assertEqual(r0[2], self.rows[0][2])
        self.assertEqual(r0[3], ts0)
        self.assertEqual(r0[4], self.rows[0][4])
        self.assertEqual(r0[5], s)
        self.assertEqual(r0[6], m)

        r1 = tsobj.rows[1]
        self.assertEqual(r1[0], self.rows[1][0])
        self.assertEqual(r1[1], self.rows[1][1])
        self.assertEqual(r1[2], self.rows[1][2])
        self.assertEqual(r1[3], ts1)
        self.assertEqual(r1[4], self.rows[1][4])
        self.assertEqual(r1[5], s)
        self.assertEqual(r1[6], m)

@unittest.skipIf(SKIP_TIMESERIES == 1, "skip requested for timeseries tests")
class TimeseriesTests(IntegrationTestBase, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.now = datetime.datetime.utcfromtimestamp(144379690)
        fiveMinsAgo = cls.now - fiveMins
        tenMinsAgo = fiveMinsAgo - fiveMins
        fifteenMinsAgo = tenMinsAgo - fiveMins
        twentyMinsAgo = fifteenMinsAgo - fiveMins

        client = RiakClient(protocol=PROTOCOL, host=HOST, http_port=HTTP_PORT,
                    pb_port=PB_PORT, credentials=SECURITY_CREDS)
        table = client.table(table_name)
        rows = [
            [ 'hash1', 'user2', twentyMinsAgo, 'hurricane', None ],
            [ 'hash1', 'user2', fifteenMinsAgo, 'rain', 79.0 ],
            [ 'hash1', 'user2', fiveMinsAgo, 'wind', 50.5 ],
            [ 'hash1', 'user2', cls.now, 'snow', 20.1 ]
        ]
        ts_obj = table.new(rows)
        result = ts_obj.store()

        codec = RiakPbcCodec()
        cls.nowMsec = codec._unix_time_millis(cls.now)
        cls.tenMinsAgoMsec = codec._unix_time_millis(tenMinsAgo)
        client.close()

    # TODO RTS-367 ts_query test. Ensure that 'None' comes back, somehow
    def test_query_that_returns_no_data(self):
        query = "select * from {} where time > 0 and time < 10 and user = 'user1'".format(table_name)
        ts_obj = self.client.ts_query('GeoCheckin', query)
        self.assertEqual(len(ts_obj.columns), 0)
        self.assertEqual(len(ts_obj.rows), 0)

    def test_query_that_matches_some_data(self):
        query = "select * from {} where time > {} and time < {} and user = 'user2'".format(table_name, self.tenMinsAgoMsec, self.nowMsec);
        ts_obj = self.client.ts_query('GeoCheckin', query)
        self.assertEqual(len(ts_obj.columns), 5)
        self.assertEqual(len(ts_obj.rows), 1)
