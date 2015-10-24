# -*- coding: utf-8 -*-
import platform
from six import PY2
from threading import Thread

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
from riak.tests.test_timeseries import TimeseriesTests

from riak.tests import HOST, PB_HOST, PB_PORT, HTTP_HOST, HTTP_PORT, \
    HAVE_PROTO, DUMMY_HTTP_PORT, DUMMY_PB_PORT, \
    SKIP_SEARCH, RUN_YZ, SECURITY_CREDS, SKIP_POOL

if PY2:
    from Queue import Queue
else:
    from queue import Queue

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest

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
                self.assertIsInstance(failure[3], StandardError)  # noqa
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
        self.assertEqual(zero_client_id, c.client_id)


# NB: no Timeseries support in HTTP
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
        for i in range(0, 300):
            link = ("other", "key%d" % i, "next")
            o.add_link(link)

        o.store()
        stored_object = bucket.get("lots_of_links")
        self.assertEqual(len(stored_object.links), 300)


if __name__ == '__main__':
    unittest.main()
