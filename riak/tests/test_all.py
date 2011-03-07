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
        result = self.client \
            .add("bucket", "foo") \
            .map("function (v) { return [JSON.parse(v.values[0].data)]; }") \
            .run()
        self.assertEqual(result, [2])

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


class RiakPbcTransportTestCase(BaseTestCase, unittest.TestCase):

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


class RiakHttpTransportTestCase(BaseTestCase, unittest.TestCase):

    def setUp(self):
        self.host = HTTP_HOST
        self.port = HTTP_PORT
        self.transport_class = RiakHttpTransport
        super(RiakHttpTransportTestCase, self).setUp()


if __name__ == '__main__':
    unittest.main()
