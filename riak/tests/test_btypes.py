import unittest

from riak import RiakError, RiakObject
from riak.bucket import RiakBucket, BucketType
from riak.tests import RUN_BTYPES
from riak.tests.base import IntegrationTestBase
from riak.tests.comparison import Comparison


@unittest.skipUnless(RUN_BTYPES, "RUN_BTYPES is 0")
class BucketTypeTests(IntegrationTestBase, unittest.TestCase, Comparison):
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

    def test_btype_get_props(self):
        defbtype = self.client.bucket_type("default")
        btype = self.client.bucket_type('no_siblings')
        with self.assertRaises(ValueError):
            defbtype.get_properties()

        props = btype.get_properties()
        self.assertIsInstance(props, dict)
        self.assertIn('n_val', props)
        self.assertEqual(3, props['n_val'])

    def test_btype_set_props(self):
        defbtype = self.client.bucket_type("default")
        btype = self.client.bucket_type('no_siblings')
        with self.assertRaises(ValueError):
            defbtype.set_properties({'allow_mult': True})

        oldprops = btype.get_properties()
        try:
            btype.set_properties({'allow_mult': True})
            newprops = btype.get_properties()
            self.assertIsInstance(newprops, dict)
            self.assertIn('allow_mult', newprops)
            self.assertTrue(newprops['allow_mult'])
            if 'claimant' in oldprops:  # HTTP hack
                del oldprops['claimant']
        finally:
            btype.set_properties(oldprops)

    def test_btype_set_props_immutable(self):
        btype = self.client.bucket_type("maps")
        with self.assertRaises(RiakError):
            btype.set_property('datatype', 'counter')

    def test_btype_list_buckets(self):
        btype = self.client.bucket_type('no_siblings')
        bucket = btype.bucket(self.bucket_name)
        obj = bucket.new(self.key_name)
        obj.data = [1, 2, 3]
        obj.store()

        self.assertIn(bucket, btype.get_buckets())
        buckets = []
        for nested_buckets in btype.stream_buckets():
            buckets.extend(nested_buckets)

        self.assertIn(bucket, buckets)

    def test_btype_list_keys(self):
        btype = self.client.bucket_type('no_siblings')
        bucket = btype.bucket(self.bucket_name)

        obj = bucket.new(self.key_name)
        obj.data = [1, 2, 3]
        obj.store()

        self.assertIn(self.key_name, bucket.get_keys())
        keys = []
        for keylist in bucket.stream_keys():
            keys.extend(keylist)

        self.assertIn(self.key_name, keys)

    def test_default_btype_list_buckets(self):
        default_btype = self.client.bucket_type("default")
        bucket = default_btype.bucket(self.bucket_name)
        obj = bucket.new(self.key_name)
        obj.data = [1, 2, 3]
        obj.store()

        self.assertIn(bucket, default_btype.get_buckets())
        buckets = []
        for nested_buckets in default_btype.stream_buckets():
            buckets.extend(nested_buckets)

        self.assertIn(bucket, buckets)

        self.assertItemsEqual(buckets, self.client.get_buckets())

    def test_default_btype_list_keys(self):
        btype = self.client.bucket_type("default")
        bucket = btype.bucket(self.bucket_name)

        obj = bucket.new(self.key_name)
        obj.data = [1, 2, 3]
        obj.store()

        self.assertIn(self.key_name, bucket.get_keys())
        keys = []
        for keylist in bucket.stream_keys():
            keys.extend(keylist)

        self.assertIn(self.key_name, keys)

        oldapikeys = self.client.get_keys(self.client.bucket(self.bucket_name))
        self.assertItemsEqual(keys, oldapikeys)

    def test_multiget_bucket_types(self):
        btype = self.client.bucket_type('no_siblings')
        bucket = btype.bucket(self.bucket_name)

        for i in range(100):
            obj = bucket.new(self.key_name + str(i))
            obj.data = {'id': i}
            obj.store()

        mget = bucket.multiget([self.key_name + str(i) for i in range(100)])
        for mobj in mget:
            self.assertIsInstance(mobj, RiakObject)
            self.assertEqual(bucket, mobj.bucket)
            self.assertEqual(btype, mobj.bucket.bucket_type)

    def test_write_once_bucket_type(self):
        bt = 'write_once'
        skey = 'write_once-init'
        btype = self.client.bucket_type(bt)
        bucket = btype.bucket(bt)
        try:
            sobj = bucket.get(skey)
        except RiakError as e:
            raise unittest.SkipTest(e)
        if not sobj.exists:
            for i in range(100):
                o = bucket.new(self.key_name + str(i))
                o.data = {'id': i}
                o.store()
            o = bucket.new(skey, data={'id': skey})
            o.store()

        mget = bucket.multiget([self.key_name + str(i) for i in range(100)])
        for mobj in mget:
            self.assertIsInstance(mobj, RiakObject)
            self.assertEqual(bucket, mobj.bucket)
            self.assertEqual(btype, mobj.bucket.bucket_type)

        props = btype.get_properties()
        self.assertIn('write_once', props)
        self.assertEqual(True, props['write_once'])
