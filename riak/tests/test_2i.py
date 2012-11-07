# -*- coding: utf-8 -*-
import os
import platform
if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest

from riak.riak_index_entry import RiakIndexEntry

SKIP_INDEXES = int(os.environ.get('SKIP_INDEXES', '0'))


class TwoITests(object):
    def is_2i_supported(self):
        # Immediate test to see if 2i is even supported w/ the backend
        try:
            self.client.index('foo', 'bar_bin', 'baz').run()
            return True
        except Exception as e:
            if "indexes_not_supported" in str(e):
                return False
            return True  # it failed, but is supported!

    @unittest.skipIf(SKIP_INDEXES, 'SKIP_INDEXES is defined')
    def test_secondary_index_store(self):
        if not self.is_2i_supported():
            return True

        # Create a new object with indexes...
        bucket = self.client.bucket('indexbucket')
        rand = self.randint()
        obj = bucket.new('mykey1', rand)
        obj.add_index('field1_bin', 'val1a')
        obj.add_index('field1_int', 1011)
        obj.store()

        # Retrieve the object, check that the correct indexes exist...
        obj = bucket.get('mykey1')
        self.assertEqual(['val1a'], sorted(obj.get_indexes('field1_bin')))
        self.assertEqual(['1011'], sorted(obj.get_indexes('field1_int')))

        # Add more indexes and save...
        obj.add_index('field1_bin', 'val1b')
        obj.add_index('field1_int', 1012)
        obj.store()

        # Retrieve the object, check that the correct indexes exist...
        obj = bucket.get('mykey1')
        self.assertEqual(['val1a', 'val1b'],
                         sorted(obj.get_indexes('field1_bin')))
        self.assertEqual(['1011', '1012'],
                         sorted(obj.get_indexes('field1_int')))

        # Check the get_indexes() function...
        self.assertEqual([
                RiakIndexEntry('field1_bin', 'val1a'),
                RiakIndexEntry('field1_bin', 'val1b'),
                RiakIndexEntry('field1_int', 1011),
                RiakIndexEntry('field1_int', 1012)
                ], sorted(obj.get_indexes()))

        # Delete an index...
        obj.remove_index('field1_bin', 'val1a')
        obj.remove_index('field1_int', 1011)
        obj.store()

        # Retrieve the object, check that the correct indexes exist...
        obj = bucket.get('mykey1')
        self.assertEqual(['val1b'], sorted(obj.get_indexes('field1_bin')))
        self.assertEqual(['1012'], sorted(obj.get_indexes('field1_int')))

        # Check duplicate entries...
        obj.add_index('field1_bin', 'val1a')
        obj.add_index('field1_bin', 'val1a')
        obj.add_index('field1_bin', 'val1a')
        obj.add_index('field1_int', 1011)
        obj.add_index('field1_int', 1011)
        obj.add_index('field1_int', 1011)

        self.assertEqual([
                RiakIndexEntry('field1_bin', 'val1a'),
                RiakIndexEntry('field1_bin', 'val1b'),
                RiakIndexEntry('field1_int', 1011),
                RiakIndexEntry('field1_int', 1012)
                ], sorted(obj.get_indexes()))

        obj.store()
        obj = bucket.get('mykey1')

        self.assertEqual([
                RiakIndexEntry('field1_bin', 'val1a'),
                RiakIndexEntry('field1_bin', 'val1b'),
                RiakIndexEntry('field1_int', 1011),
                RiakIndexEntry('field1_int', 1012)
                ], sorted(obj.get_indexes()))

        # Clean up...
        bucket.get('mykey1').delete()

    @unittest.skipIf(SKIP_INDEXES, 'SKIP_INDEXES is defined')
    def test_set_indexes(self):
        if not self.is_2i_supported():
            return True

        bucket = self.client.bucket('indexbucket')
        foo = bucket.new('foo', 1)
        foo.set_indexes((('field1_bin', 'test'), ('field2_int', 1337))).store()
        result = self.client.index('indexbucket', 'field2_int', 1337).run()
        self.assertEqual(1, len(result))
        self.assertEqual('foo', result[0].get_key())

        result = bucket.get_index('field1_bin', 'test')
        self.assertEqual(1, len(result))
        self.assertEqual('foo', str(result[0]))

    @unittest.skipIf(SKIP_INDEXES, 'SKIP_INDEXES is defined')
    def test_remove_indexes(self):
        if not self.is_2i_supported():
            return True

        bucket = self.client.bucket('indexbucket')
        bar = bucket.new('bar', 1).add_index('bar_int', 1)\
            .add_index('bar_int', 2).add_index('baz_bin', 'baz').store()
        result = bucket.get_index('bar_int', 1)
        self.assertEqual(1, len(result))
        self.assertEqual(3, len(bar.get_indexes()))
        self.assertEqual(2, len(bar.get_indexes('bar_int')))

        # remove all indexes
        bar = bar.remove_indexes().store()
        result = bucket.get_index('bar_int', 1)
        self.assertEqual(0, len(result))
        result = bucket.get_index('baz_bin', 'baz')
        self.assertEqual(0, len(result))
        self.assertEqual(0, len(bar.get_indexes()))
        self.assertEqual(0, len(bar.get_indexes('bar_int')))
        self.assertEqual(0, len(bar.get_indexes('baz_bin')))

        # add index again
        bar = bar.add_index('bar_int', 1).add_index('bar_int', 2)\
            .add_index('baz_bin', 'baz').store()
        # remove all index with field='bar_int'
        bar = bar.remove_index(field='bar_int').store()
        result = bucket.get_index('bar_int', 1)
        self.assertEqual(0, len(result))
        result = bucket.get_index('bar_int', 2)
        self.assertEqual(0, len(result))
        result = bucket.get_index('baz_bin', 'baz')
        self.assertEqual(1, len(result))
        self.assertEqual(1, len(bar.get_indexes()))
        self.assertEqual(0, len(bar.get_indexes('bar_int')))
        self.assertEqual(1, len(bar.get_indexes('baz_bin')))

        # add index again
        bar = bar.add_index('bar_int', 1).add_index('bar_int', 2)\
            .add_index('baz_bin', 'baz').store()
        # remove an index field value pair
        bar = bar.remove_index(field='bar_int', value=2).store()
        result = bucket.get_index('bar_int', 1)
        self.assertEqual(1, len(result))
        result = bucket.get_index('bar_int', 2)
        self.assertEqual(0, len(result))
        result = bucket.get_index('baz_bin', 'baz')
        self.assertEqual(1, len(result))
        self.assertEqual(2, len(bar.get_indexes()))
        self.assertEqual(1, len(bar.get_indexes('bar_int')))
        self.assertEqual(1, len(bar.get_indexes('baz_bin')))

    @unittest.skipIf(SKIP_INDEXES, 'SKIP_INDEXES is defined')
    def test_secondary_index_query(self):
        if not self.is_2i_supported():
            return True

        bucket = self.client.bucket('indexbucket')

        bucket.\
            new('mykey1', 'data1').\
            add_index('field1_bin', 'val1').\
            add_index('field2_int', 1001).\
            store()
        bucket.\
            new('mykey2', 'data1').\
            add_index('field1_bin', 'val2').\
            add_index('field2_int', 1002).\
            store()
        bucket.\
            new('mykey3', 'data1').\
            add_index('field1_bin', 'val3').\
            add_index('field2_int', 1003).\
            store()
        bucket.\
            new('mykey4', 'data1').\
            add_index('field1_bin', 'val4').\
            add_index('field2_int', 1004).\
            store()

        # Test an equality query...
        results = bucket.get_index('field1_bin', 'val2')
        self.assertEquals(1, len(results))
        self.assertEquals('mykey2', str(results[0]))

        # Test a range query...
        results = bucket.get_index('field1_bin', 'val2', 'val4')
        vals = set([str(key) for key in results])
        self.assertEquals(3, len(results))
        self.assertEquals(set(['mykey2', 'mykey3', 'mykey4']), vals)

        # Test an equality query...
        results = bucket.get_index('field2_int', 1002)
        self.assertEquals(1, len(results))
        self.assertEquals('mykey2', str(results[0]))

        # Test a range query...
        results = bucket.get_index('field2_int', 1002, 1004)
        vals = set([str(key) for key in results])
        self.assertEquals(3, len(results))
        self.assertEquals(set(['mykey2', 'mykey3', 'mykey4']), vals)

        # Clean up...
        bucket.get('mykey1').delete()
        bucket.get('mykey2').delete()
        bucket.get('mykey3').delete()
        bucket.get('mykey4').delete()
