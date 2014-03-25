import platform

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest

from . import SKIP_BTYPES
from riak.bucket import RiakBucket, BucketType


class BucketTypeTests(object):
    def test_btype_init(self):
        btype = self.client.bucket_type('foo')
        self.assertIsInstance(btype, BucketType)
        self.assertEqual('foo', btype.name)
        self.assertIs(btype, self.client.bucket_type('foo'))

    def test_btype_get_bucket(self):
        btype = self.client.bucket_type('foo')
        bucket = btype.bucket(self.bucket_name)
        self.assertIsInstance(bucket, RiakBucket)
        self.assertIs(btype, bucket.bucket_type)
        self.assertIs(bucket,
                      self.client.bucket_type('foo').bucket(self.bucket_name))
        self.assertIsNot(bucket, self.client.bucket(self.bucket_name))

    @unittest.skipIf(SKIP_BTYPES == '1', "SKIP_BTYPES is set")
    def test_btype_get_props(self):
        raise NotImplementedError('pending')

    @unittest.skipIf(SKIP_BTYPES == '1', "SKIP_BTYPES is set")
    def test_btype_set_props(self):
        raise NotImplementedError('pending')

    @unittest.skipIf(SKIP_BTYPES == '1', "SKIP_BTYPES is set")
    def test_btype_set_props_immutable(self):
        raise NotImplementedError('pending')
