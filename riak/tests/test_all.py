# -*- coding: utf-8 -*-
from __future__ import with_statement

import os
import random
import platform

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest

from riak import RiakClient
from riak.mapreduce import RiakLink
from riak import RiakKeyFilter, key_filter

from riak.test_server import TestServer

from riak.tests.test_search import SearchTests, \
    EnableSearchTests, SolrSearchTests
from riak.tests.test_mapreduce import MapReduceAliasTests, \
    ErlangMapReduceTests, JSMapReduceTests, LinkTests
from riak.tests.test_kv import BasicKVTests, KVFileTests, \
    HTTPBucketPropsTest, PbcBucketPropsTest
from riak.tests.test_2i import TwoITests

try:
    __import__('riak_pb')
    HAVE_PROTO = True
except ImportError:
    HAVE_PROTO = False

HOST = os.environ.get('RIAK_TEST_HOST', 'localhost')

PB_HOST = os.environ.get('RIAK_TEST_PB_HOST', HOST)
PB_PORT = int(os.environ.get('RIAK_TEST_PB_PORT', '8087'))

HTTP_HOST = os.environ.get('RIAK_TEST_HTTP_HOST', HOST)
HTTP_PORT = int(os.environ.get('RIAK_TEST_HTTP_PORT', '8098'))

USE_TEST_SERVER = int(os.environ.get('USE_TEST_SERVER', '0'))

if USE_TEST_SERVER:
    HTTP_PORT = 9000
    PB_PORT = 9002
    test_server = TestServer()
    test_server.cleanup()
    test_server.prepare()
    test_server.start()


class BaseTestCase(object):

    host = None
    pb_port = None
    http_port = None

    @staticmethod
    def randint():
        return random.randint(1, 999999)

    def create_client(self, host=None, http_port=None, pb_port=None,
                      protocol=None, **client_args):
        host = host or self.host or HOST
        http_port = http_port or self.http_port or HTTP_PORT
        pb_port = pb_port or self.pb_port or PB_PORT
        protocol = protocol or self.protocol
        return RiakClient(protocol=protocol,
                          host=host,
                          http_port=http_port,
                          pb_port=pb_port, **client_args)

    def setUp(self):
        self.client = self.create_client()

        # make sure these are not left over from a previous, failed run
        bucket = self.client.bucket('bucket')
        o = bucket.get('nonexistent_key_json')
        o.delete()
        o = bucket.get('nonexistent_key_binary')
        o.delete()


class RiakPbcTransportTestCase(BasicKVTests,
                               KVFileTests,
                               PbcBucketPropsTest,
                               TwoITests,
                               LinkTests,
                               ErlangMapReduceTests,
                               JSMapReduceTests,
                               MapReduceAliasTests,
                               SearchTests,
                               BaseTestCase,
                               unittest.TestCase):

    def setUp(self):
        if not HAVE_PROTO:
            self.skipTest('protobuf is unavailable')
        self.host = PB_HOST
        self.pb_port = PB_PORT
        self.protocol = 'pbc'
        super(RiakPbcTransportTestCase, self).setUp()

    def test_uses_client_id_if_given(self):
        zero_client_id = "\0\0\0\0"
        c = self.create_client(client_id=zero_client_id)
        self.assertEqual(zero_client_id, c.get_client_id())

    # def test_close_underlying_socket_fails(self):
    #     self.skipTest("TODO: No longer using connection manager, replace")
    #     c = self.create_client()

    #     bucket = c.bucket('bucket_test_close')
    #     rand = self.randint()
    #     obj = bucket.new('foo', rand)
    #     obj.store()
    #     obj = bucket.get('foo')
    #     self.assertTrue(obj.exists)
    #     self.assertEqual(obj.bucket.name, 'bucket_test_close')
    #     self.assertEqual(obj.key, 'foo')
    #     self.assertEqual(obj.data, rand)

    #     # Close the underlying socket. This gets a bit sketchy,
    #     # since we are reaching into the internals, but there is
    #     # no other way to get at the socket
    #     conns = c._cm.conns
    #     conns[0].sock.close()

    #     # This shoud fail with a socket error now
    #     self.assertRaises(socket.error, bucket.get, 'foo')

    # def test_close_underlying_socket_retry(self):
    #     self.skipTest("TODO: No longer using bare transport, replace")
    #     c = RiakClient(PB_HOST, PB_PORT, transport_class=RiakPbcTransport,
    #                                 transport_options={"max_attempts": 2})

    #     bucket = c.bucket('bucket_test_close')
    #     rand = self.randint()
    #     obj = bucket.new('barbaz', rand)
    #     obj.store()
    #     obj = bucket.get('barbaz')
    #     self.assertTrue(obj.exists)
    #     self.assertEqual(obj.bucket.name, 'bucket_test_close')
    #     self.assertEqual(obj.key, 'barbaz')
    #     self.assertEqual(obj.data, rand)

    #     # Close the underlying socket. This gets a bit sketchy,
    #     # since we are reaching into the internals, but there is
    #     # no other way to get at the socket
    #     conns = c._cm.conns
    #     conns[0].sock.close()

    #     # This should work, since we have a retry
    #     obj = bucket.get('barbaz')
    #     self.assertTrue(obj.exists)
    #     self.assertEqual(obj.bucket.name, 'bucket_test_close')
    #     self.assertEqual(obj.key, 'barbaz')
    #     self.assertEqual(obj.data, rand)

    def test_bucket_search_enabled(self):
        with self.assertRaises(NotImplementedError):
            bucket = self.client.bucket("unsearch_bucket")
            bucket.search_enabled()

    def test_enable_search_commit_hook(self):
        with self.assertRaises(NotImplementedError):
            bucket = self.client.bucket("search_bucket")
            bucket.enable_search()


class RiakHttpTransportTestCase(BasicKVTests,
                                KVFileTests,
                                HTTPBucketPropsTest,
                                TwoITests,
                                LinkTests,
                                ErlangMapReduceTests,
                                JSMapReduceTests,
                                MapReduceAliasTests,
                                EnableSearchTests,
                                SolrSearchTests,
                                SearchTests,
                                BaseTestCase,
                                unittest.TestCase):

    def setUp(self):
        self.host = HTTP_HOST
        self.http_port = HTTP_PORT
        self.protocol = 'http'
        super(RiakHttpTransportTestCase, self).setUp()

    def test_no_returnbody(self):
        bucket = self.client.bucket("bucket")
        o = bucket.new("foo", "bar").store(return_body=False)
        self.assertEqual(o.vclock, None)

    def test_too_many_link_headers_shouldnt_break_http(self):
        bucket = self.client.bucket("bucket")
        o = bucket.new("lots_of_links", "My god, it's full of links!")
        for i in range(0, 400):
            link = RiakLink("other", "key%d" % i, "next")
            o.add_link(link)

        o.store()
        stored_object = bucket.get("lots_of_links")
        self.assertEqual(len(stored_object.get_links()), 400)


class FilterTests(unittest.TestCase):
    def test_simple(self):
        f1 = RiakKeyFilter("tokenize", "-", 1)
        self.assertEqual(f1._filters, [["tokenize", "-", 1]])

    def test_add(self):
        f1 = RiakKeyFilter("tokenize", "-", 1)
        f2 = RiakKeyFilter("eq", "2005")
        f3 = f1 + f2
        self.assertEqual(list(f3), [["tokenize", "-", 1], ["eq", "2005"]])

    def test_and(self):
        f1 = RiakKeyFilter("starts_with", "2005-")
        f2 = RiakKeyFilter("ends_with", "-01")
        f3 = f1 & f2
        self.assertEqual(list(f3),
                         [["and",
                           [["starts_with", "2005-"]],
                           [["ends_with", "-01"]]]])

    def test_multi_and(self):
        f1 = RiakKeyFilter("starts_with", "2005-")
        f2 = RiakKeyFilter("ends_with", "-01")
        f3 = RiakKeyFilter("matches", "-11-")
        f4 = f1 & f2 & f3
        self.assertEqual(list(f4), [["and",
                                        [["starts_with", "2005-"]],
                                        [["ends_with", "-01"]],
                                        [["matches", "-11-"]],
                                       ]])

    def test_or(self):
        f1 = RiakKeyFilter("starts_with", "2005-")
        f2 = RiakKeyFilter("ends_with", "-01")
        f3 = f1 | f2
        self.assertEqual(list(f3), [["or", [["starts_with", "2005-"]],
                                        [["ends_with", "-01"]]]])

    def test_multi_or(self):
        f1 = RiakKeyFilter("starts_with", "2005-")
        f2 = RiakKeyFilter("ends_with", "-01")
        f3 = RiakKeyFilter("matches", "-11-")
        f4 = f1 | f2 | f3
        self.assertEqual(list(f4), [["or",
                               [["starts_with", "2005-"]],
                               [["ends_with", "-01"]],
                               [["matches", "-11-"]],
                             ]])

    def test_chaining(self):
        f1 = key_filter.tokenize("-", 1).eq("2005")
        f2 = key_filter.tokenize("-", 2).eq("05")
        f3 = f1 & f2
        self.assertEqual(list(f3), [["and",
                                     [["tokenize", "-", 1], ["eq", "2005"]],
                                     [["tokenize", "-", 2], ["eq", "05"]]
                                   ]])

if __name__ == '__main__':
    unittest.main()
