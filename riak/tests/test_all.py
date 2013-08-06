# -*- coding: utf-8 -*-
from __future__ import with_statement

import os
import random
import platform

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest

from riak.client import RiakClient
from riak.mapreduce import RiakKeyFilter
from riak import key_filter
from riak.riak_object import RiakObject

from riak.test_server import TestServer

from riak.tests.test_search import SearchTests, \
    EnableSearchTests, SolrSearchTests
from riak.tests.test_mapreduce import MapReduceAliasTests, \
    ErlangMapReduceTests, JSMapReduceTests, LinkTests, MapReduceStreamTests
from riak.tests.test_kv import BasicKVTests, KVFileTests, \
    BucketPropsTest, CounterTests
from riak.tests.test_2i import TwoITests

try:
    __import__('riak_pb')
    HAVE_PROTO = True
except ImportError:
    HAVE_PROTO = False

HOST = os.environ.get('RIAK_TEST_HOST', '127.0.0.1')

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

testrun_search_bucket = None
testrun_props_bucket = None
testrun_sibs_bucket = None


def setUpModule():
    global testrun_search_bucket, testrun_props_bucket, \
        testrun_sibs_bucket

    c = RiakClient(protocol='http', host=HTTP_HOST, http_port=HTTP_PORT)

    testrun_props_bucket = 'propsbucket'
    testrun_sibs_bucket = 'sibsbucket'
    c.bucket(testrun_sibs_bucket).allow_mult = True

    if not int(os.environ.get('SKIP_SEARCH', '0')):
        testrun_search_bucket = 'searchbucket'
        b = c.bucket(testrun_search_bucket)
        b.enable_search()


def tearDownModule():
    c = RiakClient(protocol='http', host=HTTP_HOST, http_port=HTTP_PORT)
    if not int(os.environ.get('SKIP_SEARCH', '0')):
        b = c.bucket(testrun_search_bucket)
        b.clear_properties()
    b = c.bucket(testrun_sibs_bucket)
    b.clear_properties()
    b = c.bucket(testrun_props_bucket)
    b.clear_properties()


class BaseTestCase(object):

    host = None
    pb_port = None
    http_port = None

    @staticmethod
    def randint():
        return random.randint(1, 999999)

    @staticmethod
    def randname(length=12):
        out = ''
        for i in range(length):
            out += chr(random.randint(ord('a'), ord('z')))
        return out

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
        self.bucket_name = self.randname()
        self.key_name = self.randname()
        self.search_bucket = testrun_search_bucket
        self.sibs_bucket = testrun_sibs_bucket
        self.props_bucket = testrun_props_bucket

        self.client = self.create_client()


class ClientTests(object):
    def test_request_retries(self):
        # We guess at some ports that will be unused by Riak or
        # anything else.
        client = self.create_client(http_port=1023, pb_port=1022)

        # If retries are exhausted, the final result should also be an
        # error.
        self.assertRaises(IOError, client.ping)

    def test_timeout_validation(self):
        bucket = self.client.bucket(self.bucket_name)
        key = self.key_name
        obj = bucket.new(key)
        for bad in [0, -1, False, "foo"]:
            with self.assertRaises(ValueError):
                self.client.get_buckets(timeout=bad)

            with self.assertRaises(ValueError):
                for i in self.client.stream_buckets(timeout=bad):
                    pass

            with self.assertRaises(ValueError):
                self.client.get_keys(bucket, timeout=bad)

            with self.assertRaises(ValueError):
                for i in self.client.stream_keys(bucket, timeout=bad):
                    pass

            with self.assertRaises(ValueError):
                self.client.put(obj, timeout=bad)

            with self.assertRaises(ValueError):
                self.client.get(obj, timeout=bad)

            with self.assertRaises(ValueError):
                self.client.delete(obj, timeout=bad)

            with self.assertRaises(ValueError):
                self.client.mapred([], [], bad)

            with self.assertRaises(ValueError):
                for i in self.client.stream_mapred([], [], bad):
                    pass

            with self.assertRaises(ValueError):
                self.client.get_index(bucket, 'field1_bin', 'val1', 'val4',
                                      timeout=bad)

            with self.assertRaises(ValueError):
                for i in self.client.stream_index(bucket, 'field1_bin', 'val1',
                                                  'val4', timeout=bad):
                    pass

    def test_multiget_bucket(self):
        """
        Multiget operations can be invoked on buckets.
        """
        keys = [self.key_name, self.randname(), self.randname()]
        for key in keys:
            self.client.bucket(self.bucket_name)\
                .new(key, encoded_data=key, content_type="text/plain")\
                .store()
        results = self.client.bucket(self.bucket_name).multiget(keys)
        for obj in results:
            self.assertIsInstance(obj, RiakObject)
            self.assertTrue(obj.exists)
            self.assertEqual(obj.key, obj.encoded_data)

    def test_multiget_errors(self):
        """
        Unrecoverable errors are captured along with the bucket/key
        and not propagated.
        """
        keys = [self.key_name, self.randname(), self.randname()]
        client = self.create_client(http_port=1023, pb_port=1024)
        results = client.bucket(self.bucket_name).multiget(keys)
        for failure in results:
            self.assertIsInstance(failure, tuple)
            self.assertEqual(failure[0], self.bucket_name)
            self.assertIn(failure[1], keys)
            self.assertIsInstance(failure[2], StandardError)

    def test_multiget_notfounds(self):
        """
        Not founds work in multiget just the same as get.
        """
        keys = [(self.bucket_name, self.key_name),
                (self.bucket_name, self.randname())]
        results = self.client.multiget(keys)
        for obj in results:
            self.assertIsInstance(obj, RiakObject)
            self.assertFalse(obj.exists)


class RiakPbcTransportTestCase(BasicKVTests,
                               KVFileTests,
                               BucketPropsTest,
                               TwoITests,
                               LinkTests,
                               ErlangMapReduceTests,
                               JSMapReduceTests,
                               MapReduceAliasTests,
                               MapReduceStreamTests,
                               EnableSearchTests,
                               SearchTests,
                               ClientTests,
                               CounterTests,
                               BaseTestCase,
                               unittest.TestCase):

    def setUp(self):
        if not HAVE_PROTO:
            self.skipTest('protobuf is unavailable')
        self.host = PB_HOST
        self.pb_port = PB_PORT
        self.protocol = 'pbc'
        self.http_client = self.create_client(HTTP_HOST,
                                              http_port=HTTP_PORT)
        super(RiakPbcTransportTestCase, self).setUp()

    def test_uses_client_id_if_given(self):
        zero_client_id = "\0\0\0\0"
        c = self.create_client(client_id=zero_client_id)
        self.assertEqual(zero_client_id, c.client_id)


class RiakHttpTransportTestCase(BasicKVTests,
                                KVFileTests,
                                BucketPropsTest,
                                TwoITests,
                                LinkTests,
                                ErlangMapReduceTests,
                                JSMapReduceTests,
                                MapReduceAliasTests,
                                MapReduceStreamTests,
                                EnableSearchTests,
                                SolrSearchTests,
                                SearchTests,
                                ClientTests,
                                CounterTests,
                                BaseTestCase,
                                unittest.TestCase):

    def setUp(self):
        self.host = HTTP_HOST
        self.http_port = HTTP_PORT
        self.protocol = 'http'
        super(RiakHttpTransportTestCase, self).setUp()

    def test_no_returnbody(self):
        bucket = self.client.bucket(self.bucket_name)
        o = bucket.new(self.key_name, "bar").store(return_body=False)
        self.assertEqual(o.vclock, None)

    def test_too_many_link_headers_shouldnt_break_http(self):
        bucket = self.client.bucket(self.bucket_name)
        o = bucket.new("lots_of_links", "My god, it's full of links!")
        for i in range(0, 400):
            link = ("other", "key%d" % i, "next")
            o.add_link(link)

        o.store()
        stored_object = bucket.get("lots_of_links")
        self.assertEqual(len(stored_object.links), 400)


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
