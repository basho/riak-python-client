import platform

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest

from . import SKIP_BTYPES
from riak.bucket import RiakBucket, BucketType
from riak import RiakError

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

    def test_btype_default(self):
        defbtype = self.client.bucket_type('default')
        othertype = self.client.bucket_type('foo')
        self.assertTrue(defbtype.is_default())
        self.assertFalse(othertype.is_default())

    def test_btype_repr(self):
        defbtype = self.client.bucket_type("default")
        othertype = self.client.bucket_type("foo")
        self.assertEqual("<BucketType 'default'>", str(defbtype))
        self.assertEqual("<BucketType 'foo'>", str(othertype))
        self.assertEqual("<BucketType 'default'>", repr(defbtype))
        self.assertEqual("<BucketType 'foo'>", repr(othertype))

    @unittest.skipIf(SKIP_BTYPES == '1', "SKIP_BTYPES is set")
    def test_btype_get_props(self):
        defbtype = self.client.bucket_type("default")
        btype = self.client.bucket_type("pytest")
        with self.assertRaises(ValueError):
            defbtype.get_properties()

        props = btype.get_properties()
        self.assertIsInstance(props, dict)
        self.assertIn('n_val', props)
        self.assertEqual(3, props['n_val'])

    @unittest.skipIf(SKIP_BTYPES == '1', "SKIP_BTYPES is set")
    def test_btype_set_props(self):
        defbtype = self.client.bucket_type("default")
        btype = self.client.bucket_type("pytest")
        with self.assertRaises(ValueError):
            defbtype.set_properties({'allow_mult': True})

        oldprops = btype.get_properties()
        btype.set_properties({'allow_mult': True})
        newprops = btype.get_properties()
        self.assertIsInstance(newprops, dict)
        self.assertIn('allow_mult', newprops)
        self.assertTrue(newprops['allow_mult'])
        if 'claimant' in oldprops: # HTTP hack
            del oldprops['claimant']
        btype.set_properties(oldprops)

    @unittest.skipIf(SKIP_BTYPES == '1', "SKIP_BTYPES is set")
    def test_btype_set_props_immutable(self):
        btype = self.client.bucket_type("pytest-maps")
        with self.assertRaises(RiakError):
            btype.set_property('datatype', 'counter')

    @unittest.skipIf(SKIP_BTYPES == '1', "SKIP_BTYPES is set")
    def test_btype_list_buckets(self):
        btype = self.client.bucket_type("pytest")
        bucket = btype.bucket(self.bucket_name)
        obj = bucket.new(self.key_name)
        obj.data = [1,2,3]
        obj.store()

        self.assertIn(bucket, btype.get_buckets())
        buckets = []
        for nested_buckets in btype.stream_buckets():
            buckets.extend(nested_buckets)

        self.assertIn(bucket, buckets)
