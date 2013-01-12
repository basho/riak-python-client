import platform

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest

from riak import RiakHttpTransport
from riak.client import RiakClient
from riak.riak_object import RiakObject
from riak.bucket import RiakBucket
from riak.tests.test_all import BaseTestCase


class RiakBucketRichComparisonTest(unittest.TestCase):
    def test_bucket_eq(self):
        a = RiakBucket('client', 'a')
        b = RiakBucket('client', 'a')
        self.assertEqual(a, b)

    def test_bucket_nq(self):
        a = RiakBucket('client', 'a')
        b = RiakBucket('client', 'b')
        c = RiakBucket('client', 'a')
        self.assertNotEqual(a, b, 'matched with a different bucket')

    def test_bucket_hash(self):
        a = RiakBucket('client', 'a')
        b = RiakBucket('client', 'a')
        c = RiakBucket('client', 'c')
        self.assertEqual(hash(a), hash(b), 'same bucket has different hashes')
        self.assertNotEqual(hash(a), hash(c), 'different bucket has same hash')


class RiakObjectComparisonTest(unittest.TestCase):
    def test_object_eq(self):
        a = RiakObject(None, 'bucket', 'key')
        b = RiakObject(None, 'bucket', 'key')
        self.assertEqual(a, b)

    def test_object_nq(self):
        a = RiakObject(None, 'bucket', 'key')
        b = RiakObject(None, 'bucket', 'not key')
        c = RiakObject(None, 'not bucket', 'key')
        self.assertNotEqual(a, b, 'matched with different keys')
        self.assertNotEqual(a, c, 'matched with different buckets')

    def test_object_hash(self):
        a = RiakObject(None, 'bucket', 'key')
        b = RiakObject(None, 'bucket', 'key')
        c = RiakObject(None, 'bucket', 'not key')
        self.assertEqual(hash(a), hash(b), 'same object has different hashes')
        self.assertNotEqual(hash(a), hash(c), 'different object has same hash')


class RiakClientComparisonTest(unittest.TestCase, BaseTestCase):
    def test_client_eq(self):
        self.transport_class = RiakHttpTransport
        a = self.create_client('host1', 11)
        b = self.create_client('host1', 11)
        self.assertEqual(a, b)

    def test_client_nq(self):
        self.transport_class = RiakHttpTransport
        a = self.create_client('host1', 11)
        b = self.create_client('host2', 11)
        c = self.create_client('host1', 12)
        self.assertNotEqual(a, b, 'matched with different hosts')
        self.assertNotEqual(a, c, 'matched with different ports')

    def test_client_hash(self):
        self.transport_class = RiakHttpTransport
        a = self.create_client('host1', 11)
        b = self.create_client('host1', 11)
        c = self.create_client('host2', 11)
        self.assertEqual(hash(a), hash(b), 'same object has different hashes')
        self.assertNotEqual(hash(a), hash(c), 'different object has same hash')

if __name__ == '__main__':
    unittest.main()
