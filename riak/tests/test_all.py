# -*- coding: utf-8 -*-
import random
import platform
from six import PY2
from threading import Thread
if PY2:
    from Queue import Queue
else:
    from queue import Queue
if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest

from riak import RiakError
from riak.client import RiakClient
from riak.riak_object import RiakObject

from riak.tests.test_yokozuna import YZSearchTests
from riak.tests.test_search import SearchTests, \
    EnableSearchTests, SolrSearchTests
from riak.tests.test_mapreduce import MapReduceAliasTests, \
    ErlangMapReduceTests, JSMapReduceTests, LinkTests, MapReduceStreamTests
from riak.tests.test_kv import BasicKVTests, KVFileTests, \
    BucketPropsTest, CounterTests
from riak.tests.test_2i import TwoITests
from riak.tests.test_btypes import BucketTypeTests
from riak.tests.test_security import SecurityTests
from riak.tests.test_datatypes import DatatypeIntegrationTests

from riak.tests import HOST, PB_HOST, PB_PORT, HTTP_HOST, HTTP_PORT, \
    HAVE_PROTO, DUMMY_HTTP_PORT, DUMMY_PB_PORT, \
    SKIP_SEARCH, RUN_YZ, SECURITY_CREDS, SKIP_POOL, test_six

testrun_search_bucket = None
testrun_props_bucket = None
testrun_sibs_bucket = None
testrun_yz = {'btype': None, 'bucket': None, 'index': None}
testrun_yz_index = {'btype': None, 'bucket': None, 'index': None}
testrun_yz_mr = {'btype': None, 'bucket': None, 'index': None}


def setUpModule():
    global testrun_search_bucket, testrun_props_bucket, \
        testrun_sibs_bucket, testrun_yz, testrun_yz_index, testrun_yz_mr

    c = RiakClient(host=PB_HOST, http_port=HTTP_PORT,
                   pb_port=PB_PORT, credentials=SECURITY_CREDS)

    testrun_props_bucket = 'propsbucket'
    testrun_sibs_bucket = 'sibsbucket'
    c.bucket(testrun_sibs_bucket).allow_mult = True

    if (not SKIP_SEARCH and not RUN_YZ):
        testrun_search_bucket = 'searchbucket'
        b = c.bucket(testrun_search_bucket)
        b.enable_search()

    if RUN_YZ:
        # YZ index on bucket of the same name
        testrun_yz = {'btype': None, 'bucket': 'yzbucket',
                      'index': 'yzbucket'}
        # YZ index on bucket of a different name
        testrun_yz_index = {'btype': None, 'bucket': 'yzindexbucket',
                            'index': 'yzindex'}
        # Add bucket and type for Search 2.0 -> MapReduce
        testrun_yz_mr = {'btype': 'pytest-mr', 'bucket': 'mrbucket',
                         'index': 'mrbucket'}

        for yz in (testrun_yz, testrun_yz_index, testrun_yz_mr):
            c.create_search_index(yz['index'])
            if yz['btype'] is not None:
                t = c.bucket_type(yz['btype'])
                b = t.bucket(yz['bucket'])
            else:
                b = c.bucket(yz['bucket'])
            # Keep trying to set search bucket property until it succeeds
            index_set = False
            while not index_set:
                try:
                    b.set_property('search_index', yz['index'])
                    index_set = True
                except RiakError:
                    pass


def tearDownModule():
    global testrun_search_bucket, testrun_props_bucket, \
        testrun_sibs_bucket, testrun_yz_bucket

    c = RiakClient(host=HTTP_HOST, http_port=HTTP_PORT,
                   pb_port=PB_PORT, credentials=SECURITY_CREDS)

    c.bucket(testrun_sibs_bucket).clear_properties()
    c.bucket(testrun_props_bucket).clear_properties()

    if not SKIP_SEARCH and not RUN_YZ:
        b = c.bucket(testrun_search_bucket)
        b.clear_properties()

    if RUN_YZ:
        for yz in (testrun_yz, testrun_yz_index, testrun_yz_mr):
            if yz['btype'] is not None:
                t = c.bucket_type(yz['btype'])
                b = t.bucket(yz['bucket'])
            else:
                b = c.bucket(yz['bucket'])
            b.set_property('search_index', '_dont_index_')
            c.delete_search_index(yz['index'])
            for keys in b.stream_keys():
                for key in keys:
                    b.delete(key)


class BaseTestCase(object):

    host = None
    pb_port = None
    http_port = None
    credentials = None

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
                      protocol=None, credentials=None,
                      **client_args):
        host = host or self.host or HOST
        http_port = http_port or self.http_port or HTTP_PORT
        pb_port = pb_port or self.pb_port or PB_PORT
        protocol = protocol or self.protocol
        credentials = credentials or SECURITY_CREDS
        return RiakClient(protocol=protocol,
                          host=host,
                          http_port=http_port,
                          credentials=credentials,
                          pb_port=pb_port, **client_args)

    def setUp(self):
        self.bucket_name = self.randname()
        self.key_name = self.randname()
        self.search_bucket = testrun_search_bucket
        self.sibs_bucket = testrun_sibs_bucket
        self.props_bucket = testrun_props_bucket
        self.yz = testrun_yz
        self.yz_index = testrun_yz_index
        self.yz_mr = testrun_yz_mr
        self.credentials = SECURITY_CREDS

        self.client = self.create_client()


class ClientTests(object):
    def test_request_retries(self):
        # We guess at some ports that will be unused by Riak or
        # anything else.
        client = self.create_client(http_port=DUMMY_HTTP_PORT,
                                    pb_port=DUMMY_PB_PORT)

        # If retries are exhausted, the final result should also be an
        # error.
        self.assertRaises(IOError, client.ping)

    def test_request_retries_configurable(self):
        # We guess at some ports that will be unused by Riak or
        # anything else.
        client = self.create_client(http_port=DUMMY_HTTP_PORT,
                                    pb_port=DUMMY_PB_PORT)

        # Change the retry count
        client.retries = 10
        self.assertEqual(10, client.retries)

        # The retry count should be a thread local
        retries = Queue()

        def _target():
            retries.put(client.retries)
            retries.join()

        th = Thread(target=_target)
        th.start()
        self.assertEqual(3, retries.get(block=True))
        retries.task_done()
        th.join()

        # Modify the retries in a with statement
        with client.retry_count(5):
            self.assertEqual(5, client.retries)
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
            if PY2:
                self.client.bucket(self.bucket_name)\
                    .new(key, encoded_data=key, content_type="text/plain")\
                    .store()
            else:
                self.client.bucket(self.bucket_name)\
                    .new(key, data=key,
                         content_type="text/plain").store()
        results = self.client.bucket(self.bucket_name).multiget(keys)
        for obj in results:
            self.assertIsInstance(obj, RiakObject)
            self.assertTrue(obj.exists)
            if PY2:
                self.assertEqual(obj.key, obj.encoded_data)
            else:
                self.assertEqual(obj.key, obj.data)

    def test_multiget_errors(self):
        """
        Unrecoverable errors are captured along with the bucket/key
        and not propagated.
        """
        keys = [self.key_name, self.randname(), self.randname()]
        client = self.create_client(http_port=DUMMY_HTTP_PORT,
                                    pb_port=DUMMY_PB_PORT)
        results = client.bucket(self.bucket_name).multiget(keys)
        for failure in results:
            self.assertIsInstance(failure, tuple)
            self.assertEqual(failure[0], 'default')
            self.assertEqual(failure[1], self.bucket_name)
            self.assertIn(failure[2], keys)
            if PY2:
                self.assertIsInstance(failure[3], StandardError)
            else:
                self.assertIsInstance(failure[3], Exception)

    def test_multiget_notfounds(self):
        """
        Not founds work in multiget just the same as get.
        """
        keys = [("default", self.bucket_name, self.key_name),
                ("default", self.bucket_name, self.randname())]
        results = self.client.multiget(keys)
        for obj in results:
            self.assertIsInstance(obj, RiakObject)
            self.assertFalse(obj.exists)

    def test_multiget_pool_size(self):
        """
        The pool size for multigets can be configured at client initiation
        time. Multiget still works as expected.
        """
        client = self.create_client(multiget_pool_size=2)
        self.assertEqual(2, client._multiget_pool._size)

        keys = [self.key_name, self.randname(), self.randname()]
        for key in keys:
            if PY2:
                client.bucket(self.bucket_name)\
                    .new(key, encoded_data=key, content_type="text/plain")\
                    .store()
            else:
                client.bucket(self.bucket_name)\
                    .new(key, data=key, content_type="text/plain")\
                    .store()

        results = client.bucket(self.bucket_name).multiget(keys)
        for obj in results:
            self.assertIsInstance(obj, RiakObject)
            self.assertTrue(obj.exists)
            if PY2:
                self.assertEqual(obj.key, obj.encoded_data)
            else:
                self.assertEqual(obj.key, obj.data)

    @unittest.skipIf(SKIP_POOL, 'SKIP_POOL is set')
    def test_pool_close(self):
        """
        Iterate over the connection pool and close all connections.
        """
        # Do something to add to the connection pool
        self.test_multiget_bucket()
        if self.client.protocol == 'pbc':
            self.assertGreater(len(self.client._pb_pool.resources), 1)
        else:
            self.assertGreater(len(self.client._http_pool.resources), 1)
        # Now close them all up
        self.client.close()
        self.assertEqual(len(self.client._http_pool.resources), 0)
        self.assertEqual(len(self.client._pb_pool.resources), 0)


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
                               YZSearchTests,
                               ClientTests,
                               CounterTests,
                               BucketTypeTests,
                               SecurityTests,
                               DatatypeIntegrationTests,
                               BaseTestCase,
                               unittest.TestCase,
                               test_six.Comparison):

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
                                YZSearchTests,
                                ClientTests,
                                CounterTests,
                                BucketTypeTests,
                                SecurityTests,
                                DatatypeIntegrationTests,
                                BaseTestCase,
                                unittest.TestCase,
                                test_six.Comparison):

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


if __name__ == '__main__':
    unittest.main()
