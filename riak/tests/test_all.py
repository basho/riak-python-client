# -*- coding: utf-8 -*-

import copy
import cPickle
try:
    import json
except ImportError:
    import simplejson as json
import os
import random
import sys
import unittest
from riak import RiakClient
from riak import RiakPbcTransport
from riak import RiakHttpTransport
from riak import F

HOST = os.environ.get('RIAK_TEST_HOST', 'localhost')
HTTP_HOST = os.environ.get('RIAK_TEST_HTTP_HOST', HOST)
PB_HOST = os.environ.get('RIAK_TEST_PB_HOST', HOST)
HTTP_PORT = int(os.environ.get('RIAK_TEST_HTTP_PORT', '8098'))
PB_PORT = int(os.environ.get('RIAK_TEST_PB_PORT', '8087'))
SKIP_SEARCH = int(os.environ.get('SKIP_SEARCH', '0'))

class NotJsonSerializable(object):

    def __init__(self, *args, **kwargs):
        self.args = list(args)
        self.kwargs = kwargs

    def __eq__(self, other):
        if len(self.args) != len(other.args):
            return False
        if len(self.kwargs) != len(other.kwargs):
            return False
        for name, value in self.kwargs.items():
            if other.kwargs[name] != value:
                return False
        value1_args = copy.copy(self.args)
        value2_args = copy.copy(other.args)
        value1_args.sort()
        value2_args.sort()
        for i in xrange(len(value1_args)):
            if value1_args[i] != value2_args[i]:
                return False
        return True

class BaseTestCase(object):

    @staticmethod
    def randint():
        return random.randint(1, 999999)

    def create_client(self, host=None, port=None, transport_class=None):
        host = host or self.host
        port = port or self.port
        transport_class = transport_class or self.transport_class
        return RiakClient(self.host, self.port, transport_class=self.transport_class)

    def setUp(self):
        self.client = self.create_client()

    def test_is_alive(self):
        self.assertTrue(self.client.is_alive())

    def test_store_and_get(self):
        bucket = self.client.bucket('bucket')
        rand = self.randint()
        obj = bucket.new('foo', rand)
        obj.store()
        obj = bucket.get('foo')
        self.assertTrue(obj.exists())
        self.assertEqual(obj.get_bucket().get_name(), 'bucket')
        self.assertEqual(obj.get_key(), 'foo')
        self.assertEqual(obj.get_data(), rand)

        #unicode input should raise a TypeError,
        #to avoid issues further down the line
        self.assertRaises(TypeError, self.client.bucket, u'bucket')
        self.assertRaises(TypeError, bucket.new, u'foo', 'éå')
        self.assertRaises(TypeError, bucket.new, 'foo', u'éå')
        self.assertRaises(TypeError, bucket.get, u'foo')

    def test_binary_store_and_get(self):
        bucket = self.client.bucket('bucket')
        # Store as binary, retrieve as binary, then compare...
        rand = str(self.randint())
        obj = bucket.new_binary('foo1', rand)
        obj.store()
        obj = bucket.get_binary('foo1')
        self.assertTrue(obj.exists())
        self.assertEqual(obj.get_data(), rand)
        # Store as JSON, retrieve as binary, JSON-decode, then compare...
        data = [self.randint(), self.randint(), self.randint()]
        obj = bucket.new('foo2', data)
        obj.store()
        obj = bucket.get_binary('foo2')
        self.assertEqual(data, json.loads(obj.get_data()))

    def test_custom_bucket_encoder_decoder(self):
        # Teach the bucket how to pickle
        bucket = self.client.bucket("picklin_bucket")
        bucket.set_encoder('application/x-pickle', cPickle.dumps)
        bucket.set_decoder('application/x-pickle', cPickle.loads)
        data = {'array':[1, 2, 3], 'badforjson': NotJsonSerializable(1,3)}
        obj = bucket.new("foo", data, 'application/x-pickle').store()
        obj.store()
        obj2 = bucket.get("foo")
        self.assertEqual(data, obj2.get_data())

    def test_custom_client_encoder_decoder(self):
        # Teach the bucket how to pickle
        bucket = self.client.bucket("picklin_client")
        self.client.set_encoder('application/x-pickle', cPickle.dumps)
        self.client.set_decoder('application/x-pickle', cPickle.loads)
        data = {'array':[1, 2, 3], 'badforjson':NotJsonSerializable(1,3)}
        obj = bucket.new("foo", data, 'application/x-pickle').store()
        obj.store()
        obj2 = bucket.get("foo")
        self.assertEqual(data, obj2.get_data())

    def test_unknown_content_type_encoder_decoder(self):
        # Teach the bucket how to pickle
        bucket = self.client.bucket("unknown_contenttype")
        data = "some funny data"
        obj = bucket.new("foo", data, 'application/x-frobnicator').store()
        obj.store()
        obj2 = bucket.get("foo")
        self.assertEqual(data, obj2.get_data())

    def test_missing_object(self):
        bucket = self.client.bucket('bucket')
        obj = bucket.get("missing")
        self.assertFalse(obj.exists())
        self.assertEqual(obj.get_data(), None)

    def test_delete(self):
        bucket = self.client.bucket('bucket')
        rand = self.randint()
        obj = bucket.new('foo', rand)
        obj.store()
        obj = bucket.get('foo')
        self.assertTrue(obj.exists())
        obj.delete()
        obj.reload()
        self.assertFalse(obj.exists())

    def test_set_bucket_properties(self):
        bucket = self.client.bucket('bucket')
        # Test setting allow mult...
        bucket.set_allow_multiples(True)
        self.assertTrue(bucket.get_allow_multiples())
        # Test setting nval...
        bucket.set_n_val(3)
        self.assertEqual(bucket.get_n_val(), 3)
        # Test setting multiple properties...
        bucket.set_properties({"allow_mult":False, "n_val":2})
        self.assertFalse(bucket.get_allow_multiples())
        self.assertEqual(bucket.get_n_val(), 2)

    def test_rw_settings(self):
        bucket = self.client.bucket('rwsettings')
        self.assertEqual(bucket.get_r(), "default")
        self.assertEqual(bucket.get_w(), "default")
        self.assertEqual(bucket.get_dw(), "default")
        self.assertEqual(bucket.get_rw(), "default")

        bucket.set_w(1)
        self.assertEqual(bucket.get_w(), 1)

        bucket.set_r("quorum")
        self.assertEqual(bucket.get_r(), "quorum")

        bucket.set_dw("all")
        self.assertEqual(bucket.get_dw(), "all")

        bucket.set_rw("one")
        self.assertEqual(bucket.get_rw(), "one")

    def test_siblings(self):
        # Set up the bucket, clear any existing object...
        bucket = self.client.bucket('multiBucket')
        bucket.set_allow_multiples(True)
        obj = bucket.get('foo')
        obj.delete()

        obj.reload()
        self.assertFalse(obj.exists())
        self.assertEqual(obj.get_data(), None)

        # Store the same object five times...
        vals = set()
        for i in range(5):
            other_client = self.create_client()
            other_bucket = other_client.bucket('multiBucket')
            while True:
                randval = self.randint()
                if randval not in vals:
                    break

            other_obj = other_bucket.new('foo', randval)
            other_obj.store()
            vals.add(randval)

        # Make sure the object has itself plus four siblings...
        obj.reload()
        self.assertTrue(obj.has_siblings())
        self.assertEqual(obj.get_sibling_count(), 5)

        # Get each of the values - make sure they match what was assigned
        vals2 = set()
        for i in range(5):
            vals2.add(obj.get_sibling(i).get_data())
        self.assertEqual(vals, vals2)

        # Resolve the conflict, and then do a get...
        obj3 = obj.get_sibling(3)
        obj3.store()

        obj.reload()
        self.assertEqual(obj.get_sibling_count(), 0)
        self.assertEqual(obj.get_data(), obj3.get_data())

        # Clean up for next test...
        obj.delete()

    def test_javascript_source_map(self):
        # Create the object...
        bucket = self.client.bucket("bucket")
        bucket.new("foo", 2).store()
        # Run the map...
        mr = self.client.add("bucket", "foo")
        result = mr.map(
            "function (v) { return [JSON.parse(v.values[0].data)]; }").run()
        self.assertEqual(result, [2])

        #test unicode function
        self.assertRaises(TypeError, mr.map,
            u"function (v) { return [JSON.parse(v.values[0].data)]; }")

    def test_javascript_named_map(self):
        # Create the object...
        bucket = self.client.bucket("bucket")
        bucket.new("foo", 2).store()
        # Run the map...
        result = self.client \
            .add("bucket", "foo") \
            .map("Riak.mapValuesJson") \
            .run()
        self.assertEqual(result, [2])

    def test_javascript_source_map_reduce(self):
        # Create the object...
        bucket = self.client.bucket("bucket")
        bucket.new("foo", 2).store()
        bucket.new("bar", 3).store()
        bucket.new("baz", 4).store()
        # Run the map...
        result = self.client \
            .add("bucket", "foo") \
            .add("bucket", "bar") \
            .add("bucket", "baz") \
            .map("function (v) { return [1]; }") \
            .reduce("Riak.reduceSum") \
            .run()
        self.assertEqual(result, [3])

    def test_javascript_named_map_reduce(self):
        # Create the object...
        bucket = self.client.bucket("bucket")
        bucket.new("foo", 2).store()
        bucket.new("bar", 3).store()
        bucket.new("baz", 4).store()
        # Run the map...
        result = self.client \
            .add("bucket", "foo") \
            .add("bucket", "bar") \
            .add("bucket", "baz") \
            .map("Riak.mapValuesJson") \
            .reduce("Riak.reduceSum") \
            .run()
        self.assertEqual(result, [9])

    def test_javascript_bucket_map_reduce(self):
        # Create the object...
        bucket = self.client.bucket("bucket_%s" % self.randint())
        bucket.new("foo", 2).store()
        bucket.new("bar", 3).store()
        bucket.new("baz", 4).store()
        # Run the map...
        result = self.client \
            .add(bucket.get_name()) \
            .map("Riak.mapValuesJson") \
            .reduce("Riak.reduceSum") \
            .run()
        self.assertEqual(result, [9])

    def test_javascript_arg_map_reduce(self):
        # Create the object...
        bucket = self.client.bucket("bucket")
        bucket.new("foo", 2).store()
        # Run the map...
        result = self.client \
            .add("bucket", "foo", 5) \
            .add("bucket", "foo", 10) \
            .add("bucket", "foo", 15) \
            .add("bucket", "foo", -15) \
            .add("bucket", "foo", -5) \
            .map("function(v, arg) { return [arg]; }") \
            .reduce("Riak.reduceSum") \
            .run()
        self.assertEqual(result, [10])

    def test_key_filters(self):
        bucket = self.client.bucket("kftest")
        bucket.new("basho-20101215", 1).store()
        bucket.new("google-20110103", 2).store()
        bucket.new("yahoo-20090613", 3).store()

        result = self.client \
            .add("kftest") \
            .add_key_filters([["tokenize", "-", 2]]) \
            .add_key_filter("ends_with", "0613") \
            .map("function (v, keydata) { return [v.key]; }") \
            .run()

        self.assertEqual(result, ["yahoo-20090613"])

    def test_key_filters_with_search_query(self):
        mapreduce = self.client \
            .search("kftest", "query")
        self.assertRaises(Exception, mapreduce.add_key_filters, [["tokenize", "-", 2]])
        self.assertRaises(Exception, mapreduce.add_key_filter, "ends_with", "0613")

    def test_erlang_map_reduce(self):
        # Create the object...
        bucket = self.client.bucket("bucket")
        bucket.new("foo", 2).store()
        bucket.new("bar", 2).store()
        bucket.new("baz", 4).store()
        # Run the map...
        result = self.client \
            .add("bucket", "foo") \
            .add("bucket", "bar") \
            .add("bucket", "baz") \
            .map(["riak_kv_mapreduce", "map_object_value"]) \
            .reduce(["riak_kv_mapreduce", "reduce_set_union"]) \
            .run()
        self.assertEqual(len(result), 2)

    def test_map_reduce_from_object(self):
        # Create the object...
        bucket = self.client.bucket("bucket")
        bucket.new("foo", 2).store()
        obj = bucket.get("foo")
        result = obj.map("Riak.mapValuesJson").run()
        self.assertEqual(result, [2])

    def test_store_and_get_links(self):
        # Create the object...
        bucket = self.client.bucket("bucket")
        bucket.new("foo", 2) \
            .add_link(bucket.new("foo1")) \
            .add_link(bucket.new("foo2"), "tag") \
            .add_link(bucket.new("foo3"), "tag2!@#%^&*)") \
            .store()
        obj = bucket.get("foo")
        links = obj.get_links()
        self.assertEqual(len(links), 3)

    def test_link_walking(self):
        # Create the object...
        bucket = self.client.bucket("bucket")
        bucket.new("foo", 2) \
            .add_link(bucket.new("foo1", "test1").store()) \
            .add_link(bucket.new("foo2", "test2").store(), "tag") \
            .add_link(bucket.new("foo3", "test3").store(), "tag2!@#%^&*)") \
            .store()
        obj = bucket.get("foo")
        results = obj.link("bucket").run()
        self.assertEqual(len(results), 3)
        results = obj.link("bucket", "tag").run()
        self.assertEqual(len(results), 1)

    def test_store_of_missing_object(self):
        bucket = self.client.bucket("bucket")
        # for json objects
        o = bucket.get("nonexistent_key_json")
        self.assertEqual(o.exists(), False)
        o.set_data({"foo" : "bar"})
        o = o.store()
        self.assertEqual(o.get_data(), {"foo" : "bar"})
        self.assertEqual(o.get_content_type(), "application/json")
        o.delete()
        # for binary objects
        o = bucket.get_binary("nonexistent_key_binary")
        self.assertEqual(o.exists(), False)
        o.set_data("1234567890")
        o = o.store()
        self.assertEqual(o.get_data(), "1234567890")
        self.assertEqual(o.get_content_type(), "application/octet-stream")
        o.delete()


    def test_search_integration(self):
        if SKIP_SEARCH:
            return True
        # Create some objects to search across...
        bucket = self.client.bucket("searchbucket")
        bucket.new("one", {"foo":"one", "bar":"red"}).store()
        bucket.new("two", {"foo":"two", "bar":"green"}).store()
        bucket.new("three", {"foo":"three", "bar":"blue"}).store()
        bucket.new("four", {"foo":"four", "bar":"orange"}).store()
        bucket.new("five", {"foo":"five", "bar":"yellow"}).store()

        # Run some operations...
        results = self.client.search("searchbucket", "foo:one OR foo:two").run()
        if (len(results) == 0):
            print "\n\nNot running test \"testSearchIntegration()\".\n"
            print "Please ensure that you have installed the Riak Search hook on bucket \"searchbucket\" by running \"bin/search-cmd install searchbucket\".\n\n"
            return
        self.assertEqual(len(results), 2)
        results = self.client.search("searchbucket", "(foo:one OR foo:two OR foo:three OR foo:four) AND (NOT bar:green)").run()
        self.assertEqual(len(results), 3)

    def test_store_binary_object_from_file(self):
        print __file__
        bucket = self.client.bucket('bucket')
        rand = str(self.randint())
        obj = bucket.new_binary_from_file('foo_from_file', os.path.dirname(__file__) + "/test_all.py")
        obj.store()
        obj = bucket.get_binary('foo_from_file')
        self.assertNotEqual(obj.get_data(), None)
        self.assertEqual(obj.get_content_type(), "text/x-python")

    def test_store_binary_object_from_file_should_use_default_mimetype(self):
        bucket = self.client.bucket('bucket')
        rand = str(self.randint())
        obj = bucket.new_binary_from_file('foo_from_file', os.path.dirname(__file__) + '/../../THANKS')
        obj.store()
        obj = bucket.get_binary('foo_from_file')
        self.assertEqual(obj.get_content_type(), 'application/octet-stream')

    def test_store_binary_object_from_file_should_fail_if_file_not_found(self):
        bucket = self.client.bucket('bucket')
        rand = str(self.randint())
        self.assertRaises(IOError, bucket.new_binary_from_file, 'not_found_from_file', 'FILE_NOT_FOUND')
        obj = bucket.get_binary('not_found_from_file')
        self.assertEqual(obj.get_data(), None)

    def test_list_buckets(self):
        bucket = self.client.bucket("list_bucket")
        bucket.new("one", {"foo":"one", "bar":"red"}).store()
        buckets = self.client.get_buckets()
        self.assertTrue("list_bucket" in buckets)

class MapReduceAliasTestMixIn(object):
    """This tests the map reduce aliases"""

    def test_map_values(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new_binary('one', data='value_1').store()
        bucket.new_binary('two', data='value_2').store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')

        # Use the map_values alias
        result = mr.map_values().run()

        # Sort the result so that we can have a consistent
        # expected value
        result.sort()

        self.assertEqual(result, ["value_1", "value_2"])

    def test_map_values_json(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new('one', data={'val': 'value_1'}).store()
        bucket.new('two', data={'val': 'value_2'}).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')

        # Use the map_values alias
        result = mr.map_values_json().run()

        # Sort the result so that we can have a consistent
        # expected value
        result.sort(key=lambda x: x['val'])

        self.assertEqual(result, [{'val': "value_1"}, {'val': "value_2"}])

    def test_reduce_sum(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')

        # Use the map_values alias
        result = mr.map_values_json().reduce_sum().run()

        self.assertEqual(result, [3])

    def test_reduce_min(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')

        # Use the map_values alias
        result = mr.map_values_json().reduce_min().run()

        self.assertEqual(result, [1])

    def test_reduce_max(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')

        # Use the map_values alias
        result = mr.map_values_json().reduce_max().run()

        self.assertEqual(result, [2])

    def test_reduce_sort(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new('one', data="value1").store()
        bucket.new('two', data="value2").store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')

        # Use the map_values alias
        result = mr.map_values_json().reduce_sort().run()

        self.assertEqual(result, ["value1","value2"])

    def test_reduce_sort_custom(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new('one', data="value1").store()
        bucket.new('two', data="value2").store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')

        # Use the map_values alias
        result = mr.map_values_json().reduce_sort("""function(x,y) {
           if(x == y) return 0;
           return x > y ? -1 : 1;
        }""").run()

        self.assertEqual(result, ["value2","value1"])

    def test_reduce_numeric_sort(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')

        # Use the map_values alias
        result = mr.map_values_json().reduce_numeric_sort().run()

        self.assertEqual(result, [1,2])

    def test_reduce_limit(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')

        # Use the map_values alias
        result = mr.map_values_json()\
                   .reduce_numeric_sort()\
                   .reduce_limit(1).run()

        self.assertEqual(result, [1])

    def test_reduce_slice(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')

        # Use the map_values alias
        result = mr.map_values_json()\
                   .reduce_numeric_sort()\
                   .reduce_slice(1,2).run()

        self.assertEqual(result, [2])

    def test_filter_not_found(self):
        # Add a value to the bucket
        bucket = self.client.bucket('bucket')
        bucket.new('one', data=1).store()
        bucket.new('two', data=2).store()

        # Make sure "three" does not exist
        bucket.get('three').delete()

        # Create a map reduce object and use one and two as inputs
        mr = self.client.add('bucket', 'one')\
                        .add('bucket', 'two')\
                        .add('bucket', 'three')

        # Use the map_values alias
        result = mr.map_values_json()\
                   .filter_not_found()\
                   .run()

        self.assertEqual(sorted(result), [1,2])


class RiakPbcTransportTestCase(BaseTestCase, MapReduceAliasTestMixIn,
                               unittest.TestCase):

    def setUp(self):
        self.host = PB_HOST
        self.port = PB_PORT
        self.transport_class = RiakPbcTransport
        super(RiakPbcTransportTestCase, self).setUp()

    def test_uses_client_id_if_given(self):
        self.host = PB_HOST
        self.port = PB_PORT
        zero_client_id = "\0\0\0\0"
        c = RiakClient(PB_HOST, PB_PORT,
                            transport_class = RiakPbcTransport,
                            client_id = zero_client_id)
        self.assertEqual(zero_client_id, c.get_client_id()) #


class RiakHttpTransportTestCase(BaseTestCase, MapReduceAliasTestMixIn, unittest.TestCase):

    def setUp(self):
        self.host = HTTP_HOST
        self.port = HTTP_PORT
        self.transport_class = RiakHttpTransport
        super(RiakHttpTransportTestCase, self).setUp()

    def test_no_returnbody(self):
        bucket = self.client.bucket("bucket")
        o = bucket.new("foo", "bar").store(return_body=False)
        self.assertEqual(o.vclock(), None)

    def test_generate_key(self):
        # Ensure that Riak generates a random key when
        # the key passed to bucket.new() is None.
        bucket = self.client.bucket('random_key_bucket')
        for key in bucket.get_keys():
            bucket.get(str(key)).delete()
        bucket.new(None, data={}).store()
        self.assertEqual(len(bucket.get_keys()), 1)


class RiakTestFilter(unittest.TestCase):
    def test_simple(self):
        f1 = F("tokenize", "-", 1)
        self.assertEqual(f1._filters, [["tokenize", "-", 1]])

    def test_add(self):
        f1 = F("tokenize", "-", 1)
        f2 = F("eq", "2005")
        f3 = f1 + f2
        self.assertEqual(f3._filters, [["tokenize", "-", 1], ["eq", "2005"]])

    def test_and(self):
        f1 = F("starts_with", "2005-")
        f2 = F("ends_with", "-01")
        f3 = f1 & f2
        self.assertEqual(f3._filters, [["and", [["starts_with", "2005-"]], [["ends_with", "-01"]]]])

    def test_multi_and(self):
        f1 = F("starts_with", "2005-")
        f2 = F("ends_with", "-01")
        f3 = F("matches", "-11-")
        f4 = f1 & f2 & f3
        self.assertEqual(f4._filters, [["and",
                                        [["starts_with", "2005-"]],
                                        [["ends_with", "-01"]],
                                        [["matches", "-11-"]],
                                       ]])
        
    def test_or(self):
        f1 = F("starts_with", "2005-")
        f2 = F("ends_with", "-01")
        f3 = f1 | f2
        self.assertEqual(f3._filters, [["or", [["starts_with", "2005-"]], [["ends_with", "-01"]]]])

    def test_multi_or(self):
        f1 = F("starts_with", "2005-")
        f2 = F("ends_with", "-01")
        f3 = F("matches", "-11-")
        f4 = f1 | f2 | f3
        self.assertEqual(f4._filters, [["or",
                                        [["starts_with", "2005-"]],
                                        [["ends_with", "-01"]],
                                        [["matches", "-11-"]],
                                       ]])

    def test_chaining(self):
        f1 = F().tokenize("-", 1).eq("2005")
        f2 = F().tokenize("-", 2).eq("05")
        f3 = f1 & f2
        self.assertEqual(f3._filters, [["and",
                                        [["tokenize", "-", 1], ["eq", "2005"]],
                                        [["tokenize", "-", 2], ["eq", "05"]]
                                      ]])                                        

if __name__ == '__main__':
    unittest.main()
