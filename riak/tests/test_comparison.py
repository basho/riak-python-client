"""
Copyright 2015 Basho Technologies, Inc.

This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import platform
from riak.riak_object import RiakObject
from riak.bucket import RiakBucket, BucketType
from riak.tests.test_all import BaseTestCase

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest


class BucketTypeRichComparisonTest(unittest.TestCase):
    def test_btype_eq(self):
        a = BucketType('client', 'a')
        b = BucketType('client', 'a')
        c = BucketType(None, 'a')
        d = BucketType(None, 'a')
        self.assertEqual(a, b)
        self.assertEqual(c, d)

    def test_btype_nq(self):
        a = BucketType('client', 'a')
        b = BucketType('client', 'b')
        c = BucketType(None, 'a')
        d = BucketType(None, 'a')
        self.assertNotEqual(a, b, "matched with different name, same client")
        self.assertNotEqual(a, c, "matched with different client, same name")
        self.assertNotEqual(b, d, "matched with nothing in common")

    def test_btype_hash(self):
        a = BucketType('client', 'a')
        b = BucketType('client', 'a')
        c = BucketType('client', 'c')
        d = BucketType('client2', 'a')
        self.assertEqual(hash(a), hash(b),
                         'same bucket type has different hashes')
        self.assertNotEqual(hash(a), hash(c),
                            'different bucket has same hash')
        self.assertNotEqual(hash(a), hash(d),
                            'same bucket type, different client has same hash')


class RiakBucketRichComparisonTest(unittest.TestCase):
    def test_bucket_eq(self):
        default_bt = BucketType(None, "default")
        foo_bt = BucketType(None, "foo")
        a = RiakBucket('client', 'a', default_bt)
        b = RiakBucket('client', 'a', default_bt)
        c = RiakBucket('client', 'a', foo_bt)
        d = RiakBucket('client', 'a', foo_bt)
        self.assertEqual(a, b)
        self.assertEqual(c, d)

    def test_bucket_nq(self):
        default_bt = BucketType(None, "default")
        foo_bt = BucketType(None, "foo")
        a = RiakBucket('client', 'a', default_bt)
        b = RiakBucket('client', 'b', default_bt)
        c = RiakBucket('client', 'a', foo_bt)
        self.assertNotEqual(a, b, 'matched with a different bucket')
        self.assertNotEqual(a, c, 'matched with a different bucket type')

    def test_bucket_hash(self):
        default_bt = BucketType(None, "default")
        foo_bt = BucketType(None, "foo")
        a = RiakBucket('client', 'a', default_bt)
        b = RiakBucket('client', 'a', default_bt)
        c = RiakBucket('client', 'c', default_bt)
        d = RiakBucket('client', 'a', foo_bt)
        self.assertEqual(hash(a), hash(b),
                         'same bucket has different hashes')
        self.assertNotEqual(hash(a), hash(c),
                            'different bucket has same hash')
        self.assertNotEqual(hash(a), hash(d),
                            'same bucket, different bucket type has same hash')


class RiakObjectComparisonTest(unittest.TestCase):
    def test_object_eq(self):
        a = RiakObject(None, 'bucket', 'key')
        b = RiakObject(None, 'bucket', 'key')
        self.assertEqual(a, b)
        default_bt = BucketType(None, "default")
        bucket_a = RiakBucket('client', 'a', default_bt)
        bucket_b = RiakBucket('client', 'a', default_bt)
        c = RiakObject(None, bucket_a, 'key')
        d = RiakObject(None, bucket_b, 'key')
        self.assertEqual(c, d)

    def test_object_nq(self):
        a = RiakObject(None, 'bucket', 'key')
        b = RiakObject(None, 'bucket', 'not key')
        c = RiakObject(None, 'not bucket', 'key')
        self.assertNotEqual(a, b, 'matched with different keys')
        self.assertNotEqual(a, c, 'matched with different buckets')
        default_bt = BucketType(None, "default")
        foo_bt = BucketType(None, "foo")
        bucket_a = RiakBucket('client', 'a', default_bt)
        bucket_b = RiakBucket('client', 'a', foo_bt)
        c = RiakObject(None, bucket_a, 'key')
        d = RiakObject(None, bucket_b, 'key')
        self.assertNotEqual(c, d)

    def test_object_hash(self):
        a = RiakObject(None, 'bucket', 'key')
        b = RiakObject(None, 'bucket', 'key')
        c = RiakObject(None, 'bucket', 'not key')
        self.assertEqual(hash(a), hash(b), 'same object has different hashes')
        self.assertNotEqual(hash(a), hash(c), 'different object has same hash')

        default_bt = BucketType(None, "default")
        foo_bt = BucketType(None, "foo")
        bucket_a = RiakBucket('client', 'a', default_bt)
        bucket_b = RiakBucket('client', 'a', foo_bt)
        d = RiakObject(None, bucket_a, 'key')
        e = RiakObject(None, bucket_a, 'key')
        f = RiakObject(None, bucket_b, 'key')
        g = RiakObject(None, bucket_b, 'not key')
        self.assertEqual(hash(d), hash(e),
                         'same object, same bucket_type has different hashes')
        self.assertNotEqual(hash(e), hash(f),
                            'same object, different bucket type has the '
                            'same hash')
        self.assertNotEqual(hash(d), hash(g),
                            'different object, different bucket '
                            'type has same hash')

    def test_object_valid_key(self):
        a = RiakObject(None, 'bucket', 'key')
        self.assertIsInstance(a, RiakObject, 'valid key name is rejected')
        try:
            b = RiakObject(None, 'bucket', '')
        except ValueError:
            b = None
        self.assertIsNone(b, 'empty object key not allowed')


class RiakClientComparisonTest(unittest.TestCase, BaseTestCase):
    def test_client_eq(self):
        self.protocol = 'http'
        a = self.create_client(host='host1', http_port=11)
        b = self.create_client(host='host1', http_port=11)
        self.assertEqual(a, b)

    def test_client_nq(self):
        self.protocol = 'http'
        a = self.create_client(host='host1', http_port=11)
        b = self.create_client(host='host2', http_port=11)
        c = self.create_client(host='host1', http_port=12)
        self.assertNotEqual(a, b, 'matched with different hosts')
        self.assertNotEqual(a, c, 'matched with different ports')

    def test_client_hash(self):
        self.protocol = 'http'
        a = self.create_client(host='host1', http_port=11)
        b = self.create_client(host='host1', http_port=11)
        c = self.create_client(host='host2', http_port=11)
        self.assertEqual(hash(a), hash(b), 'same object has different hashes')
        self.assertNotEqual(hash(a), hash(c), 'different object has same hash')

if __name__ == '__main__':
    unittest.main()
