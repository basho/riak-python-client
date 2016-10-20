# -*- coding: utf-8 -*-
import unittest
import riak.datatypes as datatypes

from riak import RiakError, RiakBucket, BucketType, RiakObject
from riak.tests import RUN_DATATYPES
from riak.tests.base import IntegrationTestBase
from riak.tests.comparison import Comparison


class DatatypeUnitTestBase(object):
    dtype = None
    bucket = RiakBucket(None, 'test', BucketType(None, 'datatypes'))

    def op(self, dtype):
        raise NotImplementedError

    def check_op_output(self, op):
        raise NotImplementedError

    def test_new_type_is_clean(self):
        newtype = self.dtype(self.bucket, 'key')
        self.assertIsNone(newtype.to_op())

    def test_modified_type_has_op(self):
        newtype = self.dtype(self.bucket, 'key')
        self.op(newtype)
        self.assertIsNotNone(newtype.to_op())

    def test_protected_attrs_not_settable(self):
        newtype = self.dtype(self.bucket, 'key')
        for i in ('value', 'context'):
            with self.assertRaises(AttributeError):
                setattr(newtype, i, 'foo')

    def test_modified_type_has_unmodified_value(self):
        newtype = self.dtype(self.bucket, 'key')
        oldvalue = newtype.value
        self.op(newtype)
        self.assertEqual(oldvalue, newtype.value)

    def test_op_output(self):
        newtype = self.dtype(self.bucket, 'key')
        self.op(newtype)
        op = newtype.to_op()
        self.check_op_output(op)


class FlagUnitTests(DatatypeUnitTestBase, unittest.TestCase):
    dtype = datatypes.Flag

    def op(self, dtype):
        dtype.enable()

    def check_op_output(self, op):
        self.assertEqual('enable', op)

    def test_disables_require_context(self):
        dtype = self.dtype(self.bucket, 'key')
        with self.assertRaises(datatypes.ContextRequired):
            dtype.disable()

        dtype._context = 'blah'
        dtype.disable()
        self.assertTrue(dtype.modified)


class RegisterUnitTests(DatatypeUnitTestBase, unittest.TestCase):
    dtype = datatypes.Register

    def op(self, dtype):
        dtype.assign('foobarbaz')

    def check_op_output(self, op):
        self.assertEqual(('assign', 'foobarbaz'), op)


class CounterUnitTests(DatatypeUnitTestBase, unittest.TestCase):
    dtype = datatypes.Counter

    def op(self, dtype):
        dtype.increment(5)

    def check_op_output(self, op):
        self.assertEqual(('increment', 5), op)


class SetUnitTests(DatatypeUnitTestBase, unittest.TestCase, Comparison):
    dtype = datatypes.Set

    def op(self, dtype):
        dtype._context = "foo"
        dtype.add('foo')
        dtype.discard('foo')
        dtype.add('bar')

    def check_op_output(self, op):
        self.assertIn('adds', op)
        self.assertItemsEqual(op['adds'], ['bar', 'foo'])
        self.assertIn('removes', op)
        self.assertIn('foo', op['removes'])

    def test_removes_require_context(self):
        dtype = self.dtype(self.bucket, 'key')
        with self.assertRaises(datatypes.ContextRequired):
            dtype.discard('foo')
        dtype._context = 'blah'
        dtype.discard('foo')
        self.assertTrue(dtype.modified)


class HllUnitTests(DatatypeUnitTestBase, unittest.TestCase, Comparison):
    dtype = datatypes.Hll

    def op(self, dtype):
        dtype._context = 'hll_context'
        dtype.add('foo')
        dtype.add('bar')

    def check_op_output(self, op):
        self.assertIn('adds', op)
        self.assertItemsEqual(op['adds'], ['bar', 'foo'])


class MapUnitTests(DatatypeUnitTestBase, unittest.TestCase):
    dtype = datatypes.Map

    def op(self, dtype):
        dtype.counters['a'].increment(2)
        dtype.registers['b'].assign('testing')
        dtype.flags['c'].enable()
        dtype.maps['d'][('e', 'set')].add('deep value')
        dtype.maps['f'].counters['g']
        dtype.maps['h'].maps['i'].flags['j']

    def check_op_output(self, op):
        self.assertIn(('update', ('a', 'counter'), ('increment', 2)), op)
        self.assertIn(('update', ('b', 'register'), ('assign', 'testing')), op)
        self.assertIn(('update', ('c', 'flag'), 'enable'), op)
        self.assertIn(('update', ('d', 'map'), [('update', ('e', 'set'),
                                                 {'adds': ['deep value']})]),
                      op)
        self.assertNotIn(('update', ('f', 'map'), None), op)
        self.assertNotIn(('update', ('h', 'map'), [('update', ('i', 'map'),
                                                    None)]), op)

    def test_removes_require_context(self):
        dtype = self.dtype(self.bucket, 'key')
        with self.assertRaises(datatypes.ContextRequired):
            del dtype.sets['foo']

        with self.assertRaises(datatypes.ContextRequired):
            dtype.sets['bar'].discard('xyz')

        with self.assertRaises(datatypes.ContextRequired):
            del dtype.maps['baz'].registers['quux']

        dtype._context = 'blah'
        del dtype.sets['foo']
        self.assertTrue(dtype.modified)


@unittest.skipUnless(RUN_DATATYPES, 'RUN_DATATYPES is 0')
class HllDatatypeIntegrationTests(IntegrationTestBase,
                                  unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(HllDatatypeIntegrationTests, cls).setUpClass()
        client = cls.create_client()
        try:
            btype = client.bucket_type('hlls')
            btype.get_properties()
        except RiakError as e:
            raise unittest.SkipTest(e)
        finally:
            client.close()

    def test_fetch_bucket_type_props(self):
        btype = self.client.bucket_type('hlls')
        props = btype.get_properties()
        self.assertEqual(14, props['hll_precision'])

    def test_set_same_hll_precision(self):
        btype = self.client.bucket_type('hlls')
        btype.set_property('hll_precision', 14)
        props = btype.get_properties()
        self.assertEqual(14, props['hll_precision'])

    def test_set_larger_hll_precision(self):
        btype = self.client.bucket_type('hlls')
        with self.assertRaises(RiakError):
            btype.set_property('hll_precision', 15)

    def test_set_invalid_hll_precision(self):
        btype = self.client.bucket_type('hlls')
        with self.assertRaises(ValueError):
            btype.set_property('hll_precision', 3)
        with self.assertRaises(ValueError):
            btype.set_property('hll_precision', 17)
        with self.assertRaises(ValueError):
            btype.set_property('hll_precision', 0)

    def test_dt_hll(self):
        btype = self.client.bucket_type('hlls')
        props = btype.get_properties()
        self.assertEqual(14, props['hll_precision'])
        bucket = btype.bucket(self.bucket_name)
        myhll = datatypes.Hll(bucket, self.key_name)
        myhll.add('user1')
        myhll.add('user2')
        myhll.add('foo')
        myhll.add('bar')
        myhll.add('baz')
        myhll.add('user1')
        self.assertEqual(5, len(myhll._adds))

        myhll.store()
        self.assertEqual(5, myhll.value)

        otherhll = bucket.get(self.key_name)
        self.assertEqual(5, otherhll.value)


@unittest.skipUnless(RUN_DATATYPES, 'RUN_DATATYPES is 0')
class DatatypeIntegrationTests(IntegrationTestBase,
                               unittest.TestCase,
                               Comparison):
    def test_dt_counter(self):
        btype = self.client.bucket_type('counters')
        bucket = btype.bucket(self.bucket_name)
        mycount = datatypes.Counter(bucket, self.key_name)
        mycount.increment(5)
        mycount.store()

        othercount = bucket.get(self.key_name)
        self.assertEqual(5, othercount.value)

        othercount.decrement(3)
        othercount.store(return_body=True)

        mycount.reload()
        self.assertEqual(2, mycount.value)

    def test_dt_set(self):
        btype = self.client.bucket_type('sets')
        bucket = btype.bucket(self.bucket_name)
        myset = datatypes.Set(bucket, self.key_name)
        myset.add('Sean')
        myset.add('Brett')
        myset.store()

        otherset = bucket.get(self.key_name)

        self.assertIn('Sean', otherset)
        self.assertIn('Brett', otherset)

        otherset.add('Russell')
        otherset.discard('Sean')
        otherset.store(return_body=True)

        myset.reload()
        self.assertIn('Russell', myset)
        self.assertIn('Brett', myset)
        self.assertNotIn('Sean', myset)

    def test_dt_map(self):
        btype = self.client.bucket_type('maps')
        bucket = btype.bucket(self.bucket_name)
        mymap = datatypes.Map(bucket, self.key_name)

        mymap.counters['a'].increment(2)
        mymap.registers['b'].assign('testing')
        mymap.flags['c'].enable()
        mymap.maps['d'][('e', 'set')].add('deep value')
        mymap.store()

        othermap = bucket.get(self.key_name)

        self.assertIn('a', othermap.counters)
        self.assertIn('b', othermap.registers)
        self.assertIn('c', othermap.flags)
        self.assertIn('d', othermap.maps)

        self.assertEqual(2, othermap.counters['a'].value)
        self.assertEqual('testing', othermap.registers['b'].value)
        self.assertTrue(othermap.flags['c'].value)
        self.assertEqual({('e', 'set'): frozenset(['deep value'])},
                         othermap.maps['d'].value)
        self.assertEqual(frozenset([]), othermap.sets['f'].value)

        othermap.sets['f'].add('thing1')
        othermap.sets['f'].add('thing2')
        del othermap.counters['a']
        othermap.store(return_body=True)

        mymap.reload()
        self.assertNotIn('a', mymap.counters)
        self.assertIn('f', mymap.sets)
        self.assertItemsEqual(['thing1', 'thing2'], mymap.sets['f'].value)

    def test_dt_set_remove_without_context(self):
        btype = self.client.bucket_type('sets')
        bucket = btype.bucket(self.bucket_name)
        set = datatypes.Set(bucket, self.key_name)

        set.add("X")
        set.add("Y")
        set.add("Z")
        with self.assertRaises(datatypes.ContextRequired):
            set.discard("Y")

    def test_dt_set_remove_fetching_context(self):
        btype = self.client.bucket_type('sets')
        bucket = btype.bucket(self.bucket_name)
        set = datatypes.Set(bucket, self.key_name)

        set.add('X')
        set.add('Y')
        set.store()

        set.reload()
        set.discard('bogus')
        set.store()

        set2 = bucket.get(self.key_name)
        self.assertItemsEqual(['X', 'Y'], set2.value)

    def test_dt_set_add_twice(self):
        btype = self.client.bucket_type('sets')
        bucket = btype.bucket(self.bucket_name)
        set = datatypes.Set(bucket, self.key_name)

        set.add('X')
        set.add('Y')
        set.store()

        set.reload()
        set.add('X')
        set.store()

        set2 = bucket.get(self.key_name)
        self.assertItemsEqual(['X', 'Y'], set2.value)

    def test_dt_set_add_wins_in_same_op(self):
        btype = self.client.bucket_type('sets')
        bucket = btype.bucket(self.bucket_name)
        set = datatypes.Set(bucket, self.key_name)

        set.add('X')
        set.add('Y')
        set.store()

        set.reload()
        set.add('X')
        set.discard('X')
        set.store()

        set2 = bucket.get(self.key_name)
        self.assertItemsEqual(['X', 'Y'], set2.value)

    def test_dt_set_add_wins_in_same_op_reversed(self):
        btype = self.client.bucket_type('sets')
        bucket = btype.bucket(self.bucket_name)
        set = datatypes.Set(bucket, self.key_name)

        set.add('X')
        set.add('Y')
        set.store()

        set.reload()
        set.discard('X')
        set.add('X')
        set.store()

        set2 = bucket.get(self.key_name)
        self.assertItemsEqual(['X', 'Y'], set2.value)

    def test_dt_set_remove_old_context(self):
        btype = self.client.bucket_type('sets')
        bucket = btype.bucket(self.bucket_name)
        set = datatypes.Set(bucket, self.key_name)

        set.add('X')
        set.add('Y')
        set.store()

        set.reload()

        set_parallel = datatypes.Set(bucket, self.key_name)
        set_parallel.add('Z')
        set_parallel.store()

        set.discard('Z')
        set.store()

        set2 = bucket.get(self.key_name)
        self.assertItemsEqual(['X', 'Y', 'Z'], set2.value)

    def test_dt_set_remove_updated_context(self):
        btype = self.client.bucket_type('sets')
        bucket = btype.bucket(self.bucket_name)
        set = datatypes.Set(bucket, self.key_name)

        set.add('X')
        set.add('Y')
        set.store()

        set_parallel = datatypes.Set(bucket, self.key_name)
        set_parallel.add('Z')
        set_parallel.store()

        set.reload()
        set.discard('Z')
        set.store()

        set2 = bucket.get(self.key_name)
        self.assertItemsEqual(['X', 'Y'], set2.value)

    def test_dt_map_remove_set_update_same_op(self):
        btype = self.client.bucket_type('maps')
        bucket = btype.bucket(self.bucket_name)
        map = datatypes.Map(bucket, self.key_name)

        map.sets['set'].add("X")
        map.sets['set'].add("Y")
        map.store()

        map.reload()
        del map.sets['set']
        map.sets['set'].add("Z")
        map.store()

        map2 = bucket.get(self.key_name)
        self.assertItemsEqual(["Z"], map2.sets['set'])

    def test_dt_map_remove_counter_increment_same_op(self):
        btype = self.client.bucket_type('maps')
        bucket = btype.bucket(self.bucket_name)
        map = datatypes.Map(bucket, self.key_name)

        map.counters['counter'].increment(5)
        map.store()

        map.reload()
        self.assertEqual(5, map.counters['counter'].value)
        map.counters['counter'].increment(2)
        del map.counters['counter']
        map.store()

        map2 = bucket.get(self.key_name)
        self.assertEqual(2, map2.counters['counter'].value)

    def test_dt_map_remove_map_update_same_op(self):
        btype = self.client.bucket_type('maps')
        bucket = btype.bucket(self.bucket_name)
        map = datatypes.Map(bucket, self.key_name)

        map.maps['map'].sets['set'].add("X")
        map.maps['map'].sets['set'].add("Y")
        map.store()

        map.reload()
        del map.maps['map']
        map.maps['map'].sets['set'].add("Z")
        map.store()

        map2 = bucket.get(self.key_name)
        self.assertItemsEqual(["Z"], map2.maps['map'].sets['set'])

    def test_dt_set_return_body_true_default(self):
        btype = self.client.bucket_type('sets')
        bucket = btype.bucket(self.bucket_name)
        myset = bucket.new(self.key_name)
        myset.add('X')
        myset.store(return_body=False)
        with self.assertRaises(datatypes.ContextRequired):
            myset.discard('X')

        myset.add('Y')
        myset.store()
        self.assertItemsEqual(myset.value, ['X', 'Y'])

        myset.discard('X')
        myset.store()
        self.assertItemsEqual(myset.value, ['Y'])

    def test_dt_map_return_body_true_default(self):
        btype = self.client.bucket_type('maps')
        bucket = btype.bucket(self.bucket_name)
        mymap = bucket.new(self.key_name)
        mymap.sets['a'].add('X')
        mymap.store(return_body=False)
        with self.assertRaises(datatypes.ContextRequired):
            mymap.sets['a'].discard('X')
        with self.assertRaises(datatypes.ContextRequired):
            del mymap.sets['a']

        mymap.sets['a'].add('Y')
        mymap.store()
        self.assertItemsEqual(mymap.sets['a'].value, ['X', 'Y'])

        mymap.sets['a'].discard('X')
        mymap.store()
        self.assertItemsEqual(mymap.sets['a'].value, ['Y'])

        del mymap.sets['a']
        mymap.store()

        self.assertEqual(mymap.value, {})

    def test_delete_datatype(self):
        ctype = self.client.bucket_type('counters')
        cbucket = ctype.bucket(self.bucket_name)
        counter = cbucket.new(self.key_name)
        counter.increment(5)
        counter.store()

        stype = self.client.bucket_type('sets')
        sbucket = stype.bucket(self.bucket_name)
        set_ = sbucket.new(self.key_name)
        set_.add("Brett")
        set_.store()

        mtype = self.client.bucket_type('maps')
        mbucket = mtype.bucket(self.bucket_name)
        map_ = mbucket.new(self.key_name)
        map_.sets['people'].add('Sean')
        map_.store()

        for t in [counter, set_, map_]:
            t.delete()
            obj = RiakObject(self.client, t.bucket, t.key)
            self.client.get(obj)
            self.assertFalse(obj.exists,
                             "{0} exists after deletion".format(t.type_name))
