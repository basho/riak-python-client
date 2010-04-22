#!/usr/bin/env python

import riak
try: 
        import json
except ImportError: 
        import simplejson as json
import random
import copy
import cPickle

HOST = 'localhost'
HTTP_PORT = 8098
PB_PORT = 8087
VERBOSE = True

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

# BEGIN UNIT TESTS

def test_is_alive(host, port, transport_class):
	client = riak.RiakClient(host, port, transport_class=transport_class)
	assert(client.is_alive())

def test_store_and_get(host, port, transport_class):
	client = riak.RiakClient(host, port, transport_class=transport_class)
	bucket = client.bucket('bucket')
        rand = randint()
	obj = bucket.new('foo', rand)
	obj.store()
	obj = bucket.get('foo')
	assert(obj.exists())
        assert(obj.get_bucket().get_name() == 'bucket')
        assert(obj.get_key() == 'foo')
	assert(obj.get_data() == rand)

def test_binary_store_and_get(host, port, transport_class):
	client = riak.RiakClient(host, port, transport_class=transport_class)
	bucket = client.bucket('bucket')
	# Store as binary, retrieve as binary, then compare...
        rand = str(randint())
	obj = bucket.new_binary('foo1', rand)
	obj.store()
	obj = bucket.get_binary('foo1')
	assert(obj.exists())
	assert(obj.get_data() == rand)
	# Store as JSON, retrieve as binary, JSON-decode, then compare...
	data = [randint(), randint(), randint()]
	obj = bucket.new('foo2', data)
	obj.store()
	obj = bucket.get_binary('foo2')
	assert(data == json.loads(obj.get_data()))


def test_custom_bucket_encoder_decoder(host, port, transport_class):
        # Teach the bucket how to pickle
	client = riak.RiakClient(host, port, transport_class=transport_class)
	bucket = client.bucket("picklin_bucket")
        bucket.set_encoder('application/x-pickle', cPickle.dumps)
        bucket.set_decoder('application/x-pickle', cPickle.loads)
        data = {'array':[1, 2, 3], 'badforjson':NotJsonSerializable(1,3)}
	obj = bucket.new("foo", data, 'application/x-pickle').store()
        obj.store()
        obj2 = bucket.get("foo")
        assert(data == obj2.get_data())

def test_custom_client_encoder_decoder(host, port, transport_class):
        # Teach the bucket how to pickle
	client = riak.RiakClient(host, port, transport_class=transport_class)
	bucket = client.bucket("picklin_client")
        client.set_encoder('application/x-pickle', cPickle.dumps)
        client.set_decoder('application/x-pickle', cPickle.loads)
        data = {'array':[1, 2, 3], 'badforjson':NotJsonSerializable(1,3)}
	obj = bucket.new("foo", data, 'application/x-pickle').store()
        obj.store()
        obj2 = bucket.get("foo")
        assert(data == obj2.get_data())

def test_unknown_content_type_encoder_decoder(host, port, transport_class):
        # Teach the bucket how to pickle
	client = riak.RiakClient(host, port, transport_class=transport_class)
	bucket = client.bucket("unknown_contenttype")
        data = "some funny data"
	obj = bucket.new("foo", data, 'application/x-frobnicator').store()
        obj.store()
        obj2 = bucket.get("foo")
        assert(data == obj2.get_data())

def test_missing_object(host, port, transport_class):
	client = riak.RiakClient(host, port, transport_class=transport_class)
	bucket = client.bucket('bucket')
	obj = bucket.get("missing")
	assert(not obj.exists())
	assert(obj.get_data() == None)

def test_delete(host, port, transport_class):
	client = riak.RiakClient(host, port, transport_class=transport_class)
	bucket = client.bucket('bucket')
	rand = randint()
	obj = bucket.new('foo', rand)
	obj.store()
	obj = bucket.get('foo')
	assert(obj.exists())
	obj.delete()
	obj.reload()
	assert(not obj.exists())

def test_set_bucket_properties(host, port, transport_class):
        if transport_class == riak.RiakPbcTransport:
                return None
	client = riak.RiakClient(host, port, transport_class=transport_class)
	bucket = client.bucket('bucket')
	# Test setting allow mult...
	bucket.set_allow_multiples(True)
	assert(bucket.get_allow_multiples())
	# Test setting nval...
	bucket.set_n_val(3)
	assert(bucket.get_n_val() == 3)
	# Test setting multiple properties...
	bucket.set_properties({"allow_mult":False, "n_val":2})
	assert(not bucket.get_allow_multiples())
	assert(bucket.get_n_val() == 2)

def test_siblings(host, port, transport_class):
        if transport_class == riak.RiakPbcTransport:
                return None
	# Set up the bucket, clear any existing object...
	client = riak.RiakClient(host, port, transport_class=transport_class)
	bucket = client.bucket('multiBucket')
	bucket.set_allow_multiples(True)
	obj = bucket.get('foo')
	obj.delete()

        obj.reload()
	assert(not obj.exists())
	assert(obj.get_data() == None)       

        # Store the same object five times...
        vals = set()
	for i in range(5):
		other_client = riak.RiakClient(host, port, transport_class=transport_class)
		other_bucket = other_client.bucket('multiBucket')
                while True:
                        randval = randint()
                        if randval not in vals:
                                break
                        
		other_obj = other_bucket.new('foo', randval)
		other_obj.store()
                vals.add(randval)

	# Make sure the object has itself plus four siblings...
        obj.reload()
  	assert(obj.has_siblings())
	assert(obj.get_sibling_count() == 5)

        # Get each of the values - make sure they match what was assigned
        vals2 = set()
        for i in range(5):
                vals2.add(obj.get_sibling(i).get_data())
        assert(vals == vals2)
        
	# Resolve the conflict, and then do a get...
	obj3 = obj.get_sibling(3)
	obj3.store()

	obj.reload()
	assert(obj.get_sibling_count() == 0)
	assert(obj.get_data() == obj3.get_data())

	# Clean up for next test...
	obj.delete()

def test_javascript_source_map(host, port, transport_class):
        if transport_class == riak.RiakPbcTransport:
                return None
	# Create the object...
	client = riak.RiakClient(host, port, transport_class=transport_class)
	bucket = client.bucket("bucket")
	bucket.new("foo", 2).store()
	# Run the map...
	result = client \
            .add("bucket", "foo") \
            .map("function (v) { return [JSON.parse(v.values[0].data)]; }") \
            .run()
	assert(result == [2])

def testJavascriptNamedMap(host, port, transport_class):
        if transport_class == riak.RiakPbcTransport:
                return None
	# Create the object...
	client = riak.RiakClient(host, port, transport_class=transport_class)
	bucket = client.bucket("bucket")
	bucket.new("foo", 2).store()
	# Run the map...
	result = client \
            .add("bucket", "foo") \
            .map("Riak.mapValuesJson") \
            .run()
	assert(result == [2])

def test_javascript_source_mapReduce(host, port, transport_class):
        if transport_class == riak.RiakPbcTransport:
                return None
	# Create the object...
	client = riak.RiakClient(host, port, transport_class=transport_class)
	bucket = client.bucket("bucket")
	bucket.new("foo", 2).store()
	bucket.new("bar", 3).store()
	bucket.new("baz", 4).store()
	# Run the map...
	result = client \
            .add("bucket", "foo") \
            .add("bucket", "bar") \
            .add("bucket", "baz") \
            .map("function (v) { return [1]; }") \
            .reduce("function(v) { return v.length; } ") \
            .run()
	assert(result == 3)

def test_javascript_named_map_reduce(host, port, transport_class):
        if transport_class == riak.RiakPbcTransport:
                return None
	# Create the object...
	client = riak.RiakClient(host, port, transport_class=transport_class)
	bucket = client.bucket("bucket")
	bucket.new("foo", 2).store()
	bucket.new("bar", 3).store()
	bucket.new("baz", 4).store()
	# Run the map...
	result = client \
            .add("bucket", "foo") \
            .add("bucket", "bar") \
            .add("bucket", "baz") \
            .map("Riak.mapValuesJson") \
            .reduce("Riak.reduceSum") \
            .run()
	assert(result == [9])

def testJavascriptBucketMapReduce(host, port, transport_class):
        if transport_class == riak.RiakPbcTransport:
                return None
	# Create the object...
	client = riak.RiakClient(host, port, transport_class=transport_class)
	bucket = client.bucket("bucket_" . randint())
	bucket.new("foo", 2).store()
	bucket.new("bar", 3).store()
	bucket.new("baz", 4).store()
	# Run the map...
	result = client \
            .add(bucket.name) \
            .map("Riak.mapValuesJson") \
            .reduce("Riak.reduceSum") \
            .run()
	assert(result == [9])

def test_javascript_arg_map_reduce(host, port, transport_class):
        if transport_class == riak.RiakPbcTransport:
                return None
	# Create the object...
	client = riak.RiakClient(host, port, transport_class=transport_class)
	bucket = client.bucket("bucket")
	bucket.new("foo", 2).store()
	# Run the map...
	result = client \
            .add("bucket", "foo", 5) \
            .add("bucket", "foo", 10) \
            .add("bucket", "foo", 15) \
            .add("bucket", "foo", -15) \
            .add("bucket", "foo", -5) \
            .map("function(v, arg) { return [arg]; }") \
            .reduce("Riak.reduceSum") \
            .run()
	assert(result == [10])

def test_erlang_map_reduce(host, port, transport_class):
        if transport_class == riak.RiakPbcTransport:
                return None
	# Create the object...
	client = riak.RiakClient(host, port, transport_class=transport_class)
	bucket = client.bucket("bucket")
	bucket.new("foo", 2).store()
	bucket.new("bar", 2).store()
	bucket.new("baz", 4).store()
	# Run the map...
	result = client \
            .add("bucket", "foo") \
            .add("bucket", "bar") \
            .add("bucket", "baz") \
            .map(["riak_kv_mapreduce", "map_object_value"]) \
            .reduce(["riak_kv_mapreduce", "reduce_set_union"]) \
            .run()
	assert(len(result) == 2)

def test_map_reduce_from_object(host, port, transport_class):
        if transport_class == riak.RiakPbcTransport:
                return None
	# Create the object...
	client = riak.RiakClient(host, port, transport_class=transport_class)
	bucket = client.bucket("bucket")
	bucket.new("foo", 2).store()
	obj = bucket.get("foo")
	result = obj.map("Riak.mapValuesJson").run()
	assert(result == [2])

def test_store_and_get_links(host, port, transport_class):
        if transport_class == riak.RiakPbcTransport:
                return None
	# Create the object...
	client = riak.RiakClient(host, port, transport_class=transport_class)
	bucket = client.bucket("bucket")
	bucket.new("foo", 2) \
            .add_link(bucket.new("foo1")) \
            .add_link(bucket.new("foo2"), "tag") \
            .add_link(bucket.new("foo3"), "tag2!@#%^&*)") \
            .store()
	obj = bucket.get("foo")
	links = obj.get_links()
	assert(len(links) == 3)

def test_link_walking(host, port, transport_class):
        if transport_class == riak.RiakPbcTransport:
                return None
	# Create the object...
	client = riak.RiakClient(host, port, transport_class=transport_class)
	bucket = client.bucket("bucket")
	bucket.new("foo", 2) \
            .add_link(bucket.new("foo1", "test1").store()) \
            .add_link(bucket.new("foo2", "test2").store(), "tag") \
            .add_link(bucket.new("foo3", "test3").store(), "tag2!@#%^&*)") \
            .store()
	obj = bucket.get("foo")
	results = obj.link("bucket").run()
	assert(len(results) == 3)
	results = obj.link("bucket", "tag").run()
	assert(len(results) == 1)
        


test_pass = 0
test_fail = 0

def test(function):
	global test_pass, test_fail
	try:
		apply(function, [HOST, HTTP_PORT, riak.RiakHttpTransport])
		test_pass+=1
		print "  [.] TEST PASSED (http): " + function.__name__
        except:
		test_fail+=1
		print "  [X] TEST FAILED (http): " + function.__name__
		if (VERBOSE): raise
	try:
		apply(function, [HOST, PB_PORT, riak.RiakPbcTransport])
		test_pass+=1
		print "  [.] TEST PASSED (pbc): " + function.__name__
        except:
		test_fail+=1
		print "  [X] TEST FAILED (pbc): " + function.__name__
		if (VERBOSE): raise
			

def test_summary():
	global test_pass, test_fail
	if (test_fail == 0):
		print "\nSUCCESS: Passed all " + str(test_pass) + " tests.\n"
	else:
		test_total = test_pass + test_fail

def randint():
        return random.randint(1, 999999)


# CALL THE UNIT TESTS

print("Starting Unit Tests\n---\n")
test(test_is_alive)
test(test_store_and_get)
test(test_binary_store_and_get)
test(test_custom_bucket_encoder_decoder)
test(test_custom_client_encoder_decoder)
test(test_unknown_content_type_encoder_decoder)
test(test_missing_object)
test(test_delete)
test(test_set_bucket_properties)
test(test_siblings)
test(test_javascript_source_map)
test(testJavascriptNamedMap)
test(test_javascript_source_mapReduce)
test(test_javascript_named_map_reduce)
test(test_javascript_arg_map_reduce)
test(test_erlang_map_reduce)
test(test_map_reduce_from_object)
test(test_store_and_get_links)
test(test_link_walking)
test_summary()
