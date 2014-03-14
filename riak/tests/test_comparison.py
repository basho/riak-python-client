import platform

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest

from riak.riak_object import RiakObject
from riak.bucket import RiakBucket, BucketType
from riak.tests.test_all import BaseTestCase


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
        self.assertEqual(hash(a), hash(b), 'same bucket has different hashes')
        self.assertNotEqual(hash(a), hash(c), 'different bucket has same hash')
        self.assertNotEqual(hash(a), hash(d), 'same bucket, different bucket type has same hash')

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
        self.assertEqual(a, b)

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
        self.assertNotEqual(a, b)

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
        d = RiakObject(None, default_bt, 'key')
        e = RiakObject(None, default_bt, 'key')
        f = RiakObject(None, foo_bt, 'key')
        g = RiakObject(None, foo_bt, 'not key')
        self.assertEqual(hash(d), hash(e), 'same object, same bucket_type has different hashes')
        self.assertNotEqual(hash(e), hash(f), 'same object, different bucket type has the same hash')
        self.assertNotEqual(hash(d), hash(g), 'different object, different bucket type has same hash')

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
