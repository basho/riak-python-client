# -*- coding: utf-8 -*-
from __future__ import with_statement

import os
import random
import socket
import platform

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest
import uuid
import time

from riak import RiakClient
from riak import RiakPbcTransport
from riak import RiakHttpTransport
from riak import RiakHttpsTransport
from riak.mapreduce import RiakLink
from riak import RiakKeyFilter, key_filter

from riak.test_server import TestServer

from riak.tests.test_search import SearchTests, \
    EnableSearchTests, SolrSearchTests
from riak.tests.test_mapreduce import MapReduceAliasTests, \
    ErlangMapReduceTests, JSMapReduceTests, LinkTests
from riak.tests.test_kv import BasicKVTests, KVFileTests
from riak.tests.test_2i import TwoITests

try:
    import riak_pb
    HAVE_PROTO = True
except ImportError:
    HAVE_PROTO = False

HOST = os.environ.get('RIAK_TEST_HOST', 'localhost')

PB_HOST = os.environ.get('RIAK_TEST_PB_HOST', HOST)
PB_PORT = int(os.environ.get('RIAK_TEST_PB_PORT', '8087'))

HTTP_HOST = os.environ.get('RIAK_TEST_HTTP_HOST', HOST)
HTTP_PORT = int(os.environ.get('RIAK_TEST_HTTP_PORT', '8098'))

TEST_SSL = int(os.environ.get('TEST_SSL', '0'))

SKIP_LUWAK = int(os.environ.get('SKIP_LUWAK', '0'))

USE_TEST_SERVER = int(os.environ.get('USE_TEST_SERVER', '0'))

if USE_TEST_SERVER:
    HTTP_PORT = 9000
    PB_PORT = 9002
    test_server = TestServer()
    test_server.cleanup()
    test_server.prepare()
    test_server.start()


class BaseTestCase(object):

    @staticmethod
    def randint():
        return random.randint(1, 999999)

    def create_client(self, host=None, port=None, transport_class=None):
        host = host or self.host
        port = port or self.port
        transport_class = transport_class or self.transport_class
        return RiakClient(self.host, self.port,
                          transport_class=self.transport_class)

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
        self.port = PB_PORT
        self.transport_class = RiakPbcTransport
        super(RiakPbcTransportTestCase, self).setUp()

    def test_uses_client_id_if_given(self):
        self.host = PB_HOST
        self.port = PB_PORT
        zero_client_id = "\0\0\0\0"
        c = RiakClient(PB_HOST, PB_PORT,
                       transport_class=RiakPbcTransport,
                       client_id=zero_client_id)
        self.assertEqual(zero_client_id, c.get_client_id())

    def test_close_underlying_socket_fails(self):
        c = RiakClient(PB_HOST, PB_PORT, transport_class=RiakPbcTransport)

        bucket = c.bucket('bucket_test_close')
        rand = self.randint()
        obj = bucket.new('foo', rand)
        obj.store()
        obj = bucket.get('foo')
        self.assertTrue(obj.exists())
        self.assertEqual(obj.get_bucket().get_name(), 'bucket_test_close')
        self.assertEqual(obj.get_key(), 'foo')
        self.assertEqual(obj.get_data(), rand)

        # Close the underlying socket. This gets a bit sketchy,
        # since we are reaching into the internals, but there is
        # no other way to get at the socket
        conns = c._cm.conns
        conns[0].sock.close()

        # This shoud fail with a socket error now
        self.assertRaises(socket.error, bucket.get, 'foo')

    def test_close_underlying_socket_retry(self):
        c = RiakClient(PB_HOST, PB_PORT, transport_class=RiakPbcTransport,
                                         transport_options={"max_attempts": 2})

        bucket = c.bucket('bucket_test_close')
        rand = self.randint()
        obj = bucket.new('barbaz', rand)
        obj.store()
        obj = bucket.get('barbaz')
        self.assertTrue(obj.exists())
        self.assertEqual(obj.get_bucket().get_name(), 'bucket_test_close')
        self.assertEqual(obj.get_key(), 'barbaz')
        self.assertEqual(obj.get_data(), rand)

        # Close the underlying socket. This gets a bit sketchy,
        # since we are reaching into the internals, but there is
        # no other way to get at the socket
        conns = c._cm.conns
        conns[0].sock.close()

        # This should work, since we have a retry
        obj = bucket.get('barbaz')
        self.assertTrue(obj.exists())
        self.assertEqual(obj.get_bucket().get_name(), 'bucket_test_close')
        self.assertEqual(obj.get_key(), 'barbaz')
        self.assertEqual(obj.get_data(), rand)

    def test_bucket_search_enabled(self):
        bucket = self.client.bucket("unsearch_bucket")
        self.assertRaises(NotImplementedError)

    def test_enable_search_commit_hook(self):
        bucket = self.client.bucket("search_bucket")
        bucket.enable_search()
        self.assertRaises(NotImplementedError)


class RiakHttpTransportTestCase(BasicKVTests,
                                KVFileTests,
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
        self.port = HTTP_PORT
        if TEST_SSL:
            self.transport_class = RiakHttpsTransport
        else:
            self.transport_class = RiakHttpTransport
        super(RiakHttpTransportTestCase, self).setUp()

    def test_no_returnbody(self):
        bucket = self.client.bucket("bucket")
        o = bucket.new("foo", "bar").store(return_body=False)
        self.assertEqual(o.vclock(), None)

    def test_too_many_link_headers_shouldnt_break_http(self):
        bucket = self.client.bucket("bucket")
        o = bucket.new("lots_of_links", "My god, it's full of links!")
        for i in range(0, 400):
            link = RiakLink("other", "key%d" % i, "next")
            o.add_link(link)

        o.store()
        stored_object = bucket.get("lots_of_links")
        self.assertEqual(len(stored_object.get_links()), 400)

    @unittest.skipIf(SKIP_LUWAK, 'SKIP_LUWAK is defined')
    def test_store_file_with_luwak(self):
        file = os.path.join(os.path.dirname(__file__), "test_all.py")
        with open(file, "r") as input_file:
            data = input_file.read()

        key = uuid.uuid1().hex
        self.client.store_file(key, data)

    @unittest.skipIf(SKIP_LUWAK, 'SKIP_LUWAK is defined')
    def test_store_get_file_with_luwak(self):
        file = os.path.join(os.path.dirname(__file__), "test_all.py")
        with open(file, "r") as input_file:
            data = input_file.read()

        key = uuid.uuid1().hex
        self.client.store_file(key, data)
        time.sleep(1)
        file = self.client.get_file(key)
        self.assertEquals(data, file)

    @unittest.skipIf(SKIP_LUWAK, 'SKIP_LUWAK is defined')
    def test_delete_file_with_luwak(self):
        file = os.path.join(os.path.dirname(__file__), "test_all.py")
        with open(file, "r") as input_file:
            data = input_file.read()

        key = uuid.uuid1().hex
        self.client.store_file(key, data)
        time.sleep(1)
        self.client.delete_file(key)
        time.sleep(1)
        file = self.client.get_file(key)
        self.assertIsNone(file)


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
