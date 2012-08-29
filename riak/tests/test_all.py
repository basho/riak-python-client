# -*- coding: utf-8 -*-
from __future__ import with_statement

import copy
import cPickle
try:
    import json
except ImportError:
    import simplejson as json
import os
import random
import socket
import platform

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest
import uuid
import time

from riak import RiakClient
from riak import RiakPbcTransport
from riak import RiakHttpTransport
from riak import RiakKeyFilter, key_filter
from riak.riak_index_entry import RiakIndexEntry
from riak.mapreduce import RiakLink
from riak.test_server import TestServer

try:
    import riak_pb
    HAVE_PROTO = True
except ImportError:
    HAVE_PROTO = False

HOST = os.environ.get('RIAK_TEST_HOST', 'localhost')
HTTP_HOST = os.environ.get('RIAK_TEST_HTTP_HOST', HOST)
PB_HOST = os.environ.get('RIAK_TEST_PB_HOST', HOST)
HTTP_PORT = int(os.environ.get('RIAK_TEST_HTTP_PORT', '8098'))
PB_PORT = int(os.environ.get('RIAK_TEST_PB_PORT', '8087'))
SKIP_SEARCH = int(os.environ.get('SKIP_SEARCH', '0'))
SKIP_LUWAK = int(os.environ.get('SKIP_LUWAK', '0'))
SKIP_INDEXES = int(os.environ.get('SKIP_INDEXES', '0'))
USE_TEST_SERVER = int(os.environ.get('USE_TEST_SERVER', '0'))

if USE_TEST_SERVER:
    HTTP_PORT = 9000
    PB_PORT = 9002
    test_server = TestServer()
    test_server.cleanup()
    test_server.prepare()
    test_server.start()


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
        return RiakClient(self.host, self.port,
                          transport_class=self.transport_class)

    def setUp(self):
        self.client = self.create_client()

        # make sure these are not left over from a previous, failed run
        bucket = self.client.bucket('bucket')
        o = bucket.get('nonexistent_key_json')
        o.delete()
        o = bucket.get('nonexistent_key_binary')
        o.delete()

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

        # unicode objects are fine, as long as they don't
        # contain any non-ASCII chars
        self.client.bucket(u'bucket')
        self.assertRaises(TypeError, self.client.bucket, u'búcket')
        self.assertRaises(TypeError, self.client.bucket, 'búcket')

        bucket.get(u'foo')
        self.assertRaises(TypeError, bucket.get, u'føø')
        self.assertRaises(TypeError, bucket.get, 'føø')

        self.assertRaises(TypeError, bucket.new, u'foo', 'éå')
        self.assertRaises(TypeError, bucket.new, u'foo', 'éå')
        self.assertRaises(TypeError, bucket.new, 'foo', u'éå')
        self.assertRaises(TypeError, bucket.new, 'foo', u'éå')

    def test_generate_key(self):
        # Ensure that Riak generates a random key when
        # the key passed to bucket.new() is None.
        bucket = self.client.bucket('random_key_bucket')
        existing_keys = bucket.get_keys()
        o = bucket.new(None, data={})
        self.assertIsNone(o.get_key())
        o.store()
        self.assertIsNotNone(o.get_key())
        self.assertNotIn('/', o.get_key())
        self.assertNotIn(o.get_key(), existing_keys)
        self.assertEqual(len(bucket.get_keys()), len(existing_keys) + 1)

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
        data = {'array': [1, 2, 3], 'badforjson': NotJsonSerializable(1, 3)}
        obj = bucket.new("foo", data, 'application/x-pickle').store()
        obj.store()
        obj2 = bucket.get("foo")
        self.assertEqual(data, obj2.get_data())

    def test_custom_client_encoder_decoder(self):
        # Teach the bucket how to pickle
        bucket = self.client.bucket("picklin_client")
        self.client.set_encoder('application/x-pickle', cPickle.dumps)
        self.client.set_decoder('application/x-pickle', cPickle.loads)
        data = {'array': [1, 2, 3], 'badforjson': NotJsonSerializable(1, 3)}
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
        bucket.set_properties({"allow_mult": False, "n_val": 2})
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

    def test_primary_quora(self):
        bucket = self.client.bucket('primary_quora')
        self.assertEqual(bucket.get_pr(), "default")
        self.assertEqual(bucket.get_pw(), "default")

        bucket.set_pr(1)
        self.assertEqual(bucket.get_pr(), 1)

        bucket.set_pw("quorum")
        self.assertEqual(bucket.get_pw(), "quorum")

    def test_if_none_match(self):
        bucket = self.client.bucket('if_none_match_test')
        obj = bucket.get('obj')
        obj.delete()

        obj.reload()
        self.assertFalse(obj.exists())
        obj.set_data(["first store"])
        obj.store()

        obj.set_data(["second store"])
        with self.assertRaises(Exception):
            obj.store(if_none_match=True)

    def test_siblings(self):
        # Set up the bucket, clear any existing object...
        bucket = self.client.bucket('multiBucket')
        bucket.set_allow_multiples(True)
        obj = bucket.get_binary('foo')
        # Even if it previously existed, let's store a base resolved version
        # from which we can diverge by sending a stale vclock.
        obj.set_data('start')
        obj.store()

        # Store the same object five times...
        vals = set()
        for i in range(5):
            other_client = self.create_client()
            other_bucket = other_client.bucket('multiBucket')
            while True:
                randval = self.randint()
                if randval not in vals:
                    break

            other_obj = other_bucket.new_binary('foo', str(randval))
            other_obj._vclock = obj._vclock
            other_obj.store()
            vals.add(str(randval))

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

    def test_javascript_source_map(self):
        # Create the object...
        bucket = self.client.bucket("bucket")
        bucket.new("foo", 2).store()
        # Run the map...
        mr = self.client.add("bucket", "foo")
        result = mr.map(
            "function (v) { return [JSON.parse(v.values[0].data)]; }").run()
        self.assertEqual(result, [2])

        # test ASCII-encodable unicode is accepted
        mr.map(u"function (v) { return [JSON.parse(v.values[0].data)]; }")

        # test non-ASCII-encodable unicode is rejected
        self.assertRaises(TypeError, mr.map,
            u"function (v) { /* æ */ return [JSON.parse(v.values[0].data)]; }")

        # test non-ASCII-encodable string is rejected
        self.assertRaises(TypeError, mr.map,
            "function (v) { /* æ */ return [JSON.parse(v.values[0].data)]; }")

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

    def test_key_filters_f_chain(self):
        bucket = self.client.bucket("kftest")
        bucket.new("basho-20101215", 1).store()
        bucket.new("google-20110103", 2).store()
        bucket.new("yahoo-20090613", 3).store()

        # compose a chain of key filters using f as the root of
        # two filters ANDed together to ensure that f can be the root
        # of multiple chains
        filters = key_filter.tokenize("-", 1).eq("yahoo") \
            & key_filter.tokenize("-", 2).ends_with("0613")

        result = self.client \
            .add("kftest") \
            .add_key_filters(filters) \
            .map("function (v, keydata) { return [v.key]; }") \
            .run()

        self.assertEqual(result, ["yahoo-20090613"])

    def test_key_filters_with_search_query(self):
        mapreduce = self.client.search("kftest", "query")
        self.assertRaises(Exception, mapreduce.add_key_filters,
                          [["tokenize", "-", 2]])
        self.assertRaises(Exception, mapreduce.add_key_filter,
                          "ends_with", "0613")

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
        bucket.new_binary("test_store_and_get_links", '2') \
            .add_link(bucket.new("foo1")) \
            .add_link(bucket.new("foo2"), "tag") \
            .add_link(bucket.new("foo3"), "tag2!@#%^&*)") \
            .store()
        obj = bucket.get("test_store_and_get_links")
        links = obj.get_links()
        self.assertEqual(len(links), 3)
        for l in links:
            if (l.get_key() == "foo1"):
                self.assertEqual(l.get_tag(), "bucket")
            elif (l.get_key() == "foo2"):
                self.assertEqual(l.get_tag(), "tag")
            elif (l.get_key() == "foo3"):
                self.assertEqual(l.get_tag(), "tag2!@#%^&*)")
            else:
                self.assertEqual("unknown key", l.get_key())

    def test_set_links(self):
        # Create the object
        bucket = self.client.bucket("bucket")
        bucket.new("foo", 2).set_links([bucket.new("foo1"),
            (bucket.new("foo2"), "tag"),
            RiakLink("bucket", "foo2", "tag2")]).store()
        obj = bucket.get("foo")
        links = sorted(obj.get_links(), key=lambda x: x.get_key())
        self.assertEqual(len(links), 3)
        self.assertEqual(links[0].get_key(), "foo1")
        self.assertEqual(links[1].get_key(), "foo2")
        self.assertEqual(links[1].get_tag(), "tag")
        self.assertEqual(links[2].get_key(), "foo2")
        self.assertEqual(links[2].get_tag(), "tag2")

    def test_set_links_all_links(self):
        bucket = self.client.bucket("bucket")
        foo1 = bucket.new("foo", 1)
        foo2 = bucket.new("foo2", 2).store()
        links = [RiakLink("bucket", "foo2")]
        foo1.set_links(links, True)
        links = foo1.get_links()
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0].get_key(), "foo2")

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
        o.set_data({"foo": "bar"})
        o = o.store()
        self.assertEqual(o.get_data(), {"foo": "bar"})
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

    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_solr_search_from_bucket(self):
        bucket = self.client.bucket('searchbucket')
        bucket.new("user", {"username": "roidrage"}).store()
        results = bucket.search("username:roidrage")
        self.assertEquals(1, len(results['docs']))

    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_solr_search_with_params_from_bucket(self):
        bucket = self.client.bucket('searchbucket')
        bucket.new("user", {"username": "roidrage"}).store()
        results = bucket.search("username:roidrage", wt="xml")
        self.assertEquals(1, len(results['docs']))

    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_solr_search_with_params(self):
        bucket = self.client.bucket('searchbucket')
        bucket.new("user", {"username": "roidrage"}).store()
        results = self.client.solr().search("searchbucket",
                                            "username:roidrage", wt="xml")
        self.assertEquals(1, len(results['docs']))

    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_solr_search(self):
        bucket = self.client.bucket('searchbucket')
        bucket.new("user", {"username": "roidrage"}).store()
        results = self.client.solr().search("searchbucket",
                                            "username:roidrage")
        self.assertEquals(1, len(results["docs"]))

    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_search_integration(self):
        # Create some objects to search across...
        bucket = self.client.bucket("searchbucket")
        bucket.new("one", {"foo": "one", "bar": "red"}).store()
        bucket.new("two", {"foo": "two", "bar": "green"}).store()
        bucket.new("three", {"foo": "three", "bar": "blue"}).store()
        bucket.new("four", {"foo": "four", "bar": "orange"}).store()
        bucket.new("five", {"foo": "five", "bar": "yellow"}).store()

        # Run some operations...
        results = self.client.solr().search("searchbucket",
                                            "foo:one OR foo:two")
        if (len(results) == 0):
            print "\n\nNot running test \"testSearchIntegration()\".\n"
            print """Please ensure that you have installed the Riak
            Search hook on bucket \"searchbucket\" by running
            \"bin/search-cmd install searchbucket\".\n\n"""
            return
        self.assertEqual(len(results['docs']), 2)
        query = "(foo:one OR foo:two OR foo:three OR foo:four) AND\
                 (NOT bar:green)"
        results = self.client.solr().search("searchbucket", query)
        self.assertEqual(len(results['docs']), 3)

    def test_store_binary_object_from_file(self):
        bucket = self.client.bucket('bucket')
        rand = str(self.randint())
        filepath = os.path.join(os.path.dirname(__file__), 'test_all.py')
        obj = bucket.new_binary_from_file('foo_from_file', filepath)
        obj.store()
        obj = bucket.get_binary('foo_from_file')
        self.assertNotEqual(obj.get_data(), None)
        self.assertEqual(obj.get_content_type(), "text/x-python")

    def test_store_binary_object_from_file_should_use_default_mimetype(self):
        bucket = self.client.bucket('bucket')
        rand = str(self.randint())
        filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                os.pardir, os.pardir, 'THANKS')
        obj = bucket.new_binary_from_file('foo_from_file', filepath)
        obj.store()
        obj = bucket.get_binary('foo_from_file')
        self.assertEqual(obj.get_content_type(), 'application/octet-stream')

    def test_store_metadata(self):
        bucket = self.client.bucket('bucket')
        rand = self.randint()
        obj = bucket.new('fooster', rand)
        obj.set_usermeta({'custom': 'some metadata'})
        obj.store()
        obj = bucket.get('fooster')
        self.assertEqual('some metadata', obj.get_usermeta()['custom'])

    def test_store_binary_object_from_file_should_fail_if_file_not_found(self):
        bucket = self.client.bucket('bucket')
        rand = str(self.randint())
        self.assertRaises(IOError, bucket.new_binary_from_file,
                          'not_found_from_file', 'FILE_NOT_FOUND')
        obj = bucket.get_binary('not_found_from_file')
        self.assertEqual(obj.get_data(), None)

    def test_list_buckets(self):
        bucket = self.client.bucket("list_bucket")
        bucket.new("one", {"foo": "one", "bar": "red"}).store()
        buckets = self.client.get_buckets()
        self.assertTrue("list_bucket" in buckets)

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

        self.assertEqual(result, ["value1", "value2"])

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

        self.assertEqual(result, ["value2", "value1"])

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

        self.assertEqual(result, [1, 2])

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
                   .reduce_slice(1, 2).run()

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

        self.assertEqual(sorted(result), [1, 2])


class RiakPbcTransportTestCase(BaseTestCase, MapReduceAliasTestMixIn,
                               unittest.TestCase):

    def setUp(self):
        if not HAVE_PROTO:
            self.skipTest('protobuf is unavailable')
        self.host = PB_HOST
        self.port = PB_PORT
        self.transport_class = RiakPbcTransport
        super(RiakPbcTransportTestCase, self).setUp()

    def test_uses_client_id_if_given(self):
        self.host = PB_HOST
        self.port = PB_PORT
        zero_client_id = "\0\0\0\0"
        c = RiakClient(PB_HOST, PB_PORT,
                       transport_class=RiakPbcTransport,
                       client_id=zero_client_id)
        self.assertEqual(zero_client_id, c.get_client_id())

    def test_close_underlying_socket_fails(self):
        c = RiakClient(PB_HOST, PB_PORT, transport_class=RiakPbcTransport)

        bucket = c.bucket('bucket_test_close')
        rand = self.randint()
        obj = bucket.new('foo', rand)
        obj.store()
        obj = bucket.get('foo')
        self.assertTrue(obj.exists())
        self.assertEqual(obj.get_bucket().get_name(), 'bucket_test_close')
        self.assertEqual(obj.get_key(), 'foo')
        self.assertEqual(obj.get_data(), rand)

        # Close the underlying socket. This gets a bit sketchy,
        # since we are reaching into the internals, but there is
        # no other way to get at the socket
        conns = c._cm.conns
        conns[0].sock.close()

        # This shoud fail with a socket error now
        self.assertRaises(socket.error, bucket.get, 'foo')

    def test_close_underlying_socket_retry(self):
        c = RiakClient(PB_HOST, PB_PORT, transport_class=RiakPbcTransport,
                                         transport_options={"max_attempts": 2})

        bucket = c.bucket('bucket_test_close')
        rand = self.randint()
        obj = bucket.new('barbaz', rand)
        obj.store()
        obj = bucket.get('barbaz')
        self.assertTrue(obj.exists())
        self.assertEqual(obj.get_bucket().get_name(), 'bucket_test_close')
        self.assertEqual(obj.get_key(), 'barbaz')
        self.assertEqual(obj.get_data(), rand)

        # Close the underlying socket. This gets a bit sketchy,
        # since we are reaching into the internals, but there is
        # no other way to get at the socket
        conns = c._cm.conns
        conns[0].sock.close()

        # This should work, since we have a retry
        obj = bucket.get('barbaz')
        self.assertTrue(obj.exists())
        self.assertEqual(obj.get_bucket().get_name(), 'bucket_test_close')
        self.assertEqual(obj.get_key(), 'barbaz')
        self.assertEqual(obj.get_data(), rand)


class RiakHttpTransportTestCase(BaseTestCase, MapReduceAliasTestMixIn,
                                unittest.TestCase):

    def setUp(self):
        self.host = HTTP_HOST
        self.port = HTTP_PORT
        self.transport_class = RiakHttpTransport
        super(RiakHttpTransportTestCase, self).setUp()

    def test_no_returnbody(self):
        bucket = self.client.bucket("bucket")
        o = bucket.new("foo", "bar").store(return_body=False)
        self.assertEqual(o.vclock(), None)

    def test_too_many_link_headers_shouldnt_break_http(self):
        bucket = self.client.bucket("bucket")
        o = bucket.new("lots_of_links", "My god, it's full of links!")
        for i in range(0, 400):
            link = RiakLink("other", "key%d" % i, "next")
            o.add_link(link)

        o.store()
        stored_object = bucket.get("lots_of_links")
        self.assertEqual(len(stored_object.get_links()), 400)

    def test_bucket_search_enabled(self):
        bucket = self.client.bucket("unsearch_bucket")
        self.assertFalse(bucket.search_enabled())

    def test_enable_search_commit_hook(self):
        bucket = self.client.bucket("search_bucket")
        bucket.enable_search()
        self.assertTrue(self.client.bucket("search_bucket").search_enabled())

    def test_disable_search_commit_hook(self):
        bucket = self.client.bucket("no_search_bucket")
        bucket.enable_search()
        self.assertTrue(self.client.bucket("no_search_bucket")\
                            .search_enabled())
        bucket.disable_search()
        self.assertFalse(self.client.bucket("no_search_bucket")\
                             .search_enabled())

    @unittest.skipIf(SKIP_LUWAK, 'SKIP_LUWAK is defined')
    def test_store_file_with_luwak(self):
        file = os.path.join(os.path.dirname(__file__), "test_all.py")
        with open(file, "r") as input_file:
            data = input_file.read()

        key = uuid.uuid1().hex
        self.client.store_file(key, data)

    @unittest.skipIf(SKIP_LUWAK, 'SKIP_LUWAK is defined')
    def test_store_get_file_with_luwak(self):
        file = os.path.join(os.path.dirname(__file__), "test_all.py")
        with open(file, "r") as input_file:
            data = input_file.read()

        key = uuid.uuid1().hex
        self.client.store_file(key, data)
        time.sleep(1)
        file = self.client.get_file(key)
        self.assertEquals(data, file)

    @unittest.skipIf(SKIP_LUWAK, 'SKIP_LUWAK is defined')
    def test_delete_file_with_luwak(self):
        file = os.path.join(os.path.dirname(__file__), "test_all.py")
        with open(file, "r") as input_file:
            data = input_file.read()

        key = uuid.uuid1().hex
        self.client.store_file(key, data)
        time.sleep(1)
        self.client.delete_file(key)
        time.sleep(1)
        file = self.client.get_file(key)
        self.assertIsNone(file)

    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_add_document_to_index(self):
        self.client.solr().add("searchbucket",
                               {"id": "doc", "username": "tony"})
        results = self.client.solr().search("searchbucket", "username:tony")
        self.assertEquals("tony", results['docs'][0]['username'])

    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_add_multiple_documents_to_index(self):
        self.client.solr().add("searchbucket",
                               {"id": "dizzy", "username": "dizzy"},
                               {"id": "russell", "username": "russell"})
        results = self.client.solr()\
            .search("searchbucket", "username:russell OR username:dizzy")
        self.assertEquals(2, len(results['docs']))

    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_delete_documents_from_search_by_id(self):
        self.client.solr().add("searchbucket",
                               {"id": "dizzy", "username": "dizzy"},
                               {"id": "russell", "username": "russell"})
        self.client.solr().delete("searchbucket", docs=["dizzy"])
        results = self.client.solr()\
            .search("searchbucket", "username:russell OR username:dizzy")
        self.assertEquals(1, len(results['docs']))

    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_delete_documents_from_search_by_query(self):
        self.client.solr().add("searchbucket",
                               {"id": "dizzy", "username": "dizzy"},
                               {"id": "russell", "username": "russell"})
        self.client.solr()\
            .delete("searchbucket",
                    queries=["username:dizzy", "username:russell"])
        results = self.client.solr()\
            .search("searchbucket", "username:russell OR username:dizzy")
        self.assertEquals(0, len(results['docs']))

    @unittest.skipIf(SKIP_SEARCH, 'SKIP_SEARCH is defined')
    def test_delete_documents_from_search_by_query_and_id(self):
        self.client.solr().add("searchbucket",
                               {"id": "dizzy", "username": "dizzy"},
                               {"id": "russell", "username": "russell"})
        self.client.solr().delete("searchbucket",
                                  docs=["dizzy"],
                                  queries=["username:russell"])
        results = self.client.solr()\
            .search("searchbucket",
                    "username:russell OR username:dizzy")
        self.assertEquals(0, len(results['docs']))

    def test_build_rest_path_excludes_empty_query_params(self):
        self.assertEquals(
            self.client.get_transport().build_rest_path(
                bucket=self.client.bucket("foo"),
                key="bar", params={'r': None}), "/riak/foo/bar?")


class RiakTestFilter(unittest.TestCase):
    def test_simple(self):
        f1 = RiakKeyFilter("tokenize", "-", 1)
        self.assertEqual(f1._filters, [["tokenize", "-", 1]])

    def test_add(self):
        f1 = RiakKeyFilter("tokenize", "-", 1)
        f2 = RiakKeyFilter("eq", "2005")
        f3 = f1 + f2
        self.assertEqual(list(f3), [["tokenize", "-", 1], ["eq", "2005"]])

    def test_and(self):
        f1 = RiakKeyFilter("starts_with", "2005-")
        f2 = RiakKeyFilter("ends_with", "-01")
        f3 = f1 & f2
        self.assertEqual(list(f3),
                         [["and",
                           [["starts_with", "2005-"]],
                           [["ends_with", "-01"]]]])

    def test_multi_and(self):
        f1 = RiakKeyFilter("starts_with", "2005-")
        f2 = RiakKeyFilter("ends_with", "-01")
        f3 = RiakKeyFilter("matches", "-11-")
        f4 = f1 & f2 & f3
        self.assertEqual(list(f4), [["and",
                                        [["starts_with", "2005-"]],
                                        [["ends_with", "-01"]],
                                        [["matches", "-11-"]],
                                       ]])

    def test_or(self):
        f1 = RiakKeyFilter("starts_with", "2005-")
        f2 = RiakKeyFilter("ends_with", "-01")
        f3 = f1 | f2
        self.assertEqual(list(f3), [["or", [["starts_with", "2005-"]],
                                        [["ends_with", "-01"]]]])

    def test_multi_or(self):
        f1 = RiakKeyFilter("starts_with", "2005-")
        f2 = RiakKeyFilter("ends_with", "-01")
        f3 = RiakKeyFilter("matches", "-11-")
        f4 = f1 | f2 | f3
        self.assertEqual(list(f4), [["or",
                               [["starts_with", "2005-"]],
                               [["ends_with", "-01"]],
                               [["matches", "-11-"]],
                             ]])

    def test_chaining(self):
        f1 = key_filter.tokenize("-", 1).eq("2005")
        f2 = key_filter.tokenize("-", 2).eq("05")
        f3 = f1 & f2
        self.assertEqual(list(f3), [["and",
                                     [["tokenize", "-", 1], ["eq", "2005"]],
                                     [["tokenize", "-", 2], ["eq", "05"]]
                                   ]])

if __name__ == '__main__':
    unittest.main()
