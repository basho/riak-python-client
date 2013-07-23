# -*- coding: utf-8 -*-
import os
import cPickle
import copy
import platform
from time import sleep
from riak import ConflictError
from riak.resolver import default_resolver, last_written_resolver
try:
    import simplejson as json
except ImportError:
    import json

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest


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


class BasicKVTests(object):
    def test_is_alive(self):
        self.assertTrue(self.client.is_alive())

    def test_store_and_get(self):
        bucket = self.client.bucket(self.bucket_name)
        rand = self.randint()
        obj = bucket.new('foo', rand)
        obj.store()
        obj = bucket.get('foo')
        self.assertTrue(obj.exists)
        self.assertEqual(obj.bucket.name, self.bucket_name)
        self.assertEqual(obj.key, 'foo')
        self.assertEqual(obj.data, rand)

        # unicode objects are fine, as long as they don't
        # contain any non-ASCII chars
        self.client.bucket(unicode(self.bucket_name))
        self.assertRaises(TypeError, self.client.bucket, u'búcket')
        self.assertRaises(TypeError, self.client.bucket, 'búcket')

        bucket.get(u'foo')
        self.assertRaises(TypeError, bucket.get, u'føø')
        self.assertRaises(TypeError, bucket.get, 'føø')

        self.assertRaises(TypeError, bucket.new, u'foo', 'éå')
        self.assertRaises(TypeError, bucket.new, u'foo', 'éå')
        self.assertRaises(TypeError, bucket.new, 'foo', u'éå')
        self.assertRaises(TypeError, bucket.new, 'foo', u'éå')

        obj2 = bucket.new('baz', rand, 'application/json')
        obj2.charset = 'UTF-8'
        obj2.store()
        obj2 = bucket.get('baz')
        self.assertEqual(obj2.data, rand)

    def test_store_obj_with_unicode(self):
        bucket = self.client.bucket(self.bucket_name)
        data = {u'føø': u'éå'}
        obj = bucket.new('foo', data)
        obj.store()
        obj = bucket.get('foo')
        self.assertEqual(obj.data, data)

    def test_store_unicode_string(self):
        bucket = self.client.bucket(self.bucket_name)
        data = u"some unicode data: \u00c6"
        obj = bucket.new(self.key_name, encoded_data=data.encode('utf-8'),
                         content_type='text/plain')
        obj.charset = 'utf-8'
        obj.store()
        obj2 = bucket.get(self.key_name)
        self.assertEqual(data, obj2.encoded_data.decode('utf-8'))

    def test_generate_key(self):
        # Ensure that Riak generates a random key when
        # the key passed to bucket.new() is None.
        bucket = self.client.bucket('random_key_bucket')
        existing_keys = bucket.get_keys()
        o = bucket.new(None, data={})
        self.assertIsNone(o.key)
        o.store()
        self.assertIsNotNone(o.key)
        self.assertNotIn('/', o.key)
        self.assertNotIn(o.key, existing_keys)
        self.assertEqual(len(bucket.get_keys()), len(existing_keys) + 1)

    def test_stream_keys(self):
        bucket = self.client.bucket('random_key_bucket')
        regular_keys = bucket.get_keys()
        self.assertNotEqual(len(regular_keys), 0)
        streamed_keys = []
        for keylist in bucket.stream_keys():
            self.assertNotEqual([], keylist)
            for key in keylist:
                self.assertIsInstance(key, basestring)
            streamed_keys += keylist
        self.assertEqual(sorted(regular_keys), sorted(streamed_keys))

    def test_stream_keys_abort(self):
        bucket = self.client.bucket('random_key_bucket')
        regular_keys = bucket.get_keys()
        self.assertNotEqual(len(regular_keys), 0)
        try:
            for keylist in bucket.stream_keys():
                raise RuntimeError("abort")
        except RuntimeError:
            pass

        # If the stream was closed correctly, this will not error
        robj = bucket.get(regular_keys[0])
        self.assertEqual(len(robj.siblings), 1)
        self.assertEqual(True, robj.exists)

    def test_bad_key(self):
        bucket = self.client.bucket(self.bucket_name)
        obj = bucket.new()
        with self.assertRaises(TypeError):
            bucket.get(None)

        with self.assertRaises(TypeError):
            self.client.get(obj)

        with self.assertRaises(TypeError):
            bucket.get(1)

    def test_binary_store_and_get(self):
        bucket = self.client.bucket(self.bucket_name)
        # Store as binary, retrieve as binary, then compare...
        rand = str(self.randint())
        obj = bucket.new(self.key_name, encoded_data=rand,
                         content_type='text/plain')
        obj.store()
        obj = bucket.get(self.key_name)
        self.assertTrue(obj.exists)
        self.assertEqual(obj.encoded_data, rand)
        # Store as JSON, retrieve as binary, JSON-decode, then compare...
        data = [self.randint(), self.randint(), self.randint()]
        key2 = self.randname()
        obj = bucket.new(key2, data)
        obj.store()
        obj = bucket.get(key2)
        self.assertEqual(data, json.loads(obj.encoded_data))

    def test_blank_binary_204(self):
        bucket = self.client.bucket(self.bucket_name)

        # this should *not* raise an error
        obj = bucket.new('foo2', encoded_data='', content_type='text/plain')
        obj.store()
        obj = bucket.get('foo2')
        self.assertTrue(obj.exists)
        self.assertEqual(obj.encoded_data, '')

    def test_custom_bucket_encoder_decoder(self):
        bucket = self.client.bucket(self.bucket_name)
        # Teach the bucket how to pickle
        bucket.set_encoder('application/x-pickle', cPickle.dumps)
        bucket.set_decoder('application/x-pickle', cPickle.loads)
        data = {'array': [1, 2, 3], 'badforjson': NotJsonSerializable(1, 3)}
        obj = bucket.new(self.key_name, data, 'application/x-pickle')
        obj.store()
        obj2 = bucket.get(self.key_name)
        self.assertEqual(data, obj2.data)

    def test_custom_client_encoder_decoder(self):
        bucket = self.client.bucket(self.bucket_name)
        # Teach the client how to pickle
        self.client.set_encoder('application/x-pickle', cPickle.dumps)
        self.client.set_decoder('application/x-pickle', cPickle.loads)
        data = {'array': [1, 2, 3], 'badforjson': NotJsonSerializable(1, 3)}
        obj = bucket.new(self.key_name, data, 'application/x-pickle')
        obj.store()
        obj2 = bucket.get(self.key_name)
        self.assertEqual(data, obj2.data)

    def test_unknown_content_type_encoder_decoder(self):
        # Teach the bucket how to pickle
        bucket = self.client.bucket(self.bucket_name)
        data = "some funny data"
        obj = bucket.new(self.key_name,
                         encoded_data=data,
                         content_type='application/x-frobnicator')
        obj.store()
        obj2 = bucket.get(self.key_name)
        self.assertEqual(data, obj2.encoded_data)

    def test_text_plain_encoder_decoder(self):
        bucket = self.client.bucket(self.bucket_name)
        data = "some funny data"
        obj = bucket.new(self.key_name, data, content_type='text/plain')
        obj.store()
        obj2 = bucket.get(self.key_name)
        self.assertEqual(data, obj2.data)

    def test_missing_object(self):
        bucket = self.client.bucket(self.bucket_name)
        obj = bucket.get(self.key_name)
        self.assertFalse(obj.exists)
        # Object with no siblings should not raise the ConflictError
        self.assertIsNone(obj.data)

    def test_delete(self):
        bucket = self.client.bucket(self.bucket_name)
        rand = self.randint()
        obj = bucket.new(self.key_name, rand)
        obj.store()
        obj = bucket.get(self.key_name)
        self.assertTrue(obj.exists)

        obj.delete()
        obj.reload()
        self.assertFalse(obj.exists)

    def test_bucket_delete(self):
        bucket = self.client.bucket(self.bucket_name)
        rand = self.randint()
        obj = bucket.new(self.key_name, rand)
        obj.store()

        bucket.delete(self.key_name)
        obj.reload()
        self.assertFalse(obj.exists)

    def test_set_bucket_properties(self):
        bucket = self.client.bucket(self.props_bucket)
        # Test setting allow mult...
        bucket.allow_mult = True
        # Test setting nval...
        bucket.n_val = 1

        bucket2 = self.create_client().bucket(self.props_bucket)
        self.assertTrue(bucket2.allow_mult)
        self.assertEqual(bucket2.n_val, 1)
        # Test setting multiple properties...
        bucket.set_properties({"allow_mult": False, "n_val": 2})

        bucket3 = self.create_client().bucket(self.props_bucket)
        self.assertFalse(bucket3.allow_mult)
        self.assertEqual(bucket3.n_val, 2)

    def test_if_none_match(self):
        bucket = self.client.bucket(self.bucket_name)
        obj = bucket.get(self.key_name)
        obj.delete()

        obj.reload()
        self.assertFalse(obj.exists)
        obj.data = ["first store"]
        obj.content_type = 'application/json'
        obj.store()

        obj.data = ["second store"]
        with self.assertRaises(Exception):
            obj.store(if_none_match=True)

    def test_siblings(self):
        # Set up the bucket, clear any existing object...
        bucket = self.client.bucket(self.sibs_bucket)
        obj = bucket.get(self.key_name)
        bucket.allow_mult = True

        # Even if it previously existed, let's store a base resolved version
        # from which we can diverge by sending a stale vclock.
        obj.encoded_data = 'start'
        obj.content_type = 'application/octet-stream'
        obj.store()

        vals = set(self.generate_siblings(obj, count=5))

        # Make sure the object has five siblings...
        obj = bucket.get(self.key_name)
        obj.reload()
        self.assertEqual(len(obj.siblings), 5)

        # When the object is in conflict, using the shortcut methods
        # should raise the ConflictError
        with self.assertRaises(ConflictError):
            obj.data

        # Get each of the values - make sure they match what was
        # assigned
        vals2 = set([sibling.encoded_data for sibling in obj.siblings])
        self.assertEqual(vals, vals2)

        # Resolve the conflict, and then do a get...
        resolved_sibling = obj.siblings[3]
        obj.siblings = [resolved_sibling]
        obj.store()

        obj.reload()
        self.assertEqual(len(obj.siblings), 1)
        self.assertEqual(obj.encoded_data, resolved_sibling.encoded_data)

    @unittest.skipIf(os.environ.get('SKIP_RESOLVE', '0') == '1',
                     "skip requested for resolvers test")
    def test_resolution(self):
        bucket = self.client.bucket(self.sibs_bucket)
        obj = bucket.get(self.key_name)
        bucket.allow_mult = True

        # Even if it previously existed, let's store a base resolved version
        # from which we can diverge by sending a stale vclock.
        obj.encoded_data = 'start'
        obj.content_type = 'text/plain'
        obj.store()

        vals = self.generate_siblings(obj, count=5, delay=1.01)

        # Make sure the object has five siblings when using the
        # default resolver
        obj = bucket.get(self.key_name)
        obj.reload()
        self.assertEqual(len(obj.siblings), 5)

        # Setting the resolver on the client object to use the
        # "last-write-wins" behavior
        self.client.resolver = last_written_resolver
        obj.reload()
        self.assertEqual(obj.resolver, last_written_resolver)
        self.assertEqual(1, len(obj.siblings))
        self.assertEqual(obj.data, vals[-1])

        # Set the resolver on the bucket to the default resolver,
        # overriding the resolver on the client
        bucket.resolver = default_resolver
        obj.reload()
        self.assertEqual(obj.resolver, default_resolver)
        self.assertEqual(len(obj.siblings), 5)

        # Define our own custom resolver on the object that returns
        # the maximum value, overriding the bucket and client resolvers
        def max_value_resolver(obj):
            datafun = lambda s: s.data
            obj.siblings = [max(obj.siblings, key=datafun), ]

        obj.resolver = max_value_resolver
        obj.reload()
        self.assertEqual(obj.resolver, max_value_resolver)
        self.assertEqual(obj.data, max(vals))

    def test_tombstone_siblings(self):
        # Set up the bucket, clear any existing object...
        bucket = self.client.bucket(self.sibs_bucket)
        obj = bucket.get(self.key_name)
        bucket.allow_mult = True

        obj.encoded_data = 'start'
        obj.content_type = 'application/octet-stream'
        obj.store(return_body=True)

        obj.delete()

        vals = set(self.generate_siblings(obj, count=4))

        obj = bucket.get(self.key_name)
        self.assertEqual(len(obj.siblings), 5)
        non_tombstones = 0
        for sib in obj.siblings:
            if sib.exists:
                non_tombstones += 1
            self.assertTrue(sib.encoded_data in vals or not sib.exists)
        self.assertEqual(non_tombstones, 4)

    def test_store_of_missing_object(self):
        bucket = self.client.bucket(self.bucket_name)
        # for json objects
        o = bucket.get(self.key_name)
        self.assertEqual(o.exists, False)
        o.data = {"foo": "bar"}
        o.content_type = 'application/json'

        o = o.store()
        self.assertEqual(o.data, {"foo": "bar"})
        self.assertEqual(o.content_type, "application/json")
        o.delete()
        # for binary objects
        o = bucket.get(self.randname())
        self.assertEqual(o.exists, False)
        o.encoded_data = "1234567890"
        o.content_type = 'application/octet-stream'

        o = o.store()
        self.assertEqual(o.encoded_data, "1234567890")
        self.assertEqual(o.content_type, "application/octet-stream")
        o.delete()

    def test_store_metadata(self):
        bucket = self.client.bucket(self.bucket_name)
        rand = self.randint()
        obj = bucket.new(self.key_name, rand)
        obj.usermeta = {'custom': 'some metadata'}
        obj.store()
        obj = bucket.get(self.key_name)
        self.assertEqual('some metadata', obj.usermeta['custom'])

    def test_list_buckets(self):
        bucket = self.client.bucket(self.bucket_name)
        bucket.new("one", {"foo": "one", "bar": "red"}).store()
        buckets = self.client.get_buckets()
        self.assertTrue(self.bucket_name in [x.name for x in buckets])

    def test_stream_buckets(self):
        bucket = self.client.bucket(self.bucket_name)
        bucket.new(self.key_name, data={"foo": "one",
                                        "bar": "baz"}).store()
        buckets = []
        for bucket_list in self.client.stream_buckets():
            buckets.extend(bucket_list)

        self.assertTrue(self.bucket_name in [x.name for x in buckets])

    def test_stream_buckets_abort(self):
        bucket = self.client.bucket(self.bucket_name)
        bucket.new(self.key_name, data={"foo": "one",
                                        "bar": "baz"}).store()
        try:
            for bucket_list in self.client.stream_buckets():
                raise RuntimeError("abort")
        except RuntimeError:
            pass

        robj = bucket.get(self.key_name)
        self.assertTrue(robj.exists)
        self.assertEqual(len(robj.siblings), 1)

    def generate_siblings(self, original, count=5, delay=None):
        vals = []
        for i in range(count):
            while True:
                randval = self.randint()
                if str(randval) not in vals:
                    break

            other_obj = original.bucket.new(key=original.key,
                                            encoded_data=str(randval),
                                            content_type='text/plain')
            other_obj.vclock = original.vclock
            other_obj.store()
            vals.append(str(randval))
            if delay:
                sleep(delay)
        return vals


class BucketPropsTest(object):
    def test_rw_settings(self):
        bucket = self.client.bucket(self.props_bucket)
        self.assertEqual(bucket.r, "quorum")
        self.assertEqual(bucket.w, "quorum")
        self.assertEqual(bucket.dw, "quorum")
        self.assertEqual(bucket.rw, "quorum")

        bucket.w = 1
        self.assertEqual(bucket.w, 1)

        bucket.r = "quorum"
        self.assertEqual(bucket.r, "quorum")

        bucket.dw = "all"
        self.assertEqual(bucket.dw, "all")

        bucket.rw = "one"
        self.assertEqual(bucket.rw, "one")

        bucket.set_properties({'w': 'quorum',
                               'r': 'quorum',
                               'dw': 'quorum',
                               'rw': 'quorum'})
        bucket.clear_properties()

    def test_primary_quora(self):
        bucket = self.client.bucket(self.props_bucket)
        self.assertEqual(bucket.pr, 0)
        self.assertEqual(bucket.pw, 0)

        bucket.pr = 1
        self.assertEqual(bucket.pr, 1)

        bucket.pw = "quorum"
        self.assertEqual(bucket.pw, "quorum")

        bucket.set_properties({'pr': 0, 'pw': 0})
        bucket.clear_properties()

    def test_clear_bucket_properties(self):
        bucket = self.client.bucket(self.props_bucket)
        bucket.allow_mult = True
        self.assertTrue(bucket.allow_mult)
        bucket.n_val = 1
        self.assertEqual(bucket.n_val, 1)
        # Test setting clearing properties...

        self.assertTrue(bucket.clear_properties())
        self.assertFalse(bucket.allow_mult)
        self.assertEqual(bucket.n_val, 3)


class KVFileTests(object):
    def test_store_binary_object_from_file(self):
        bucket = self.client.bucket(self.bucket_name)
        filepath = os.path.join(os.path.dirname(__file__), 'test_all.py')
        obj = bucket.new_from_file(self.key_name, filepath)
        obj.store()
        obj = bucket.get(self.key_name)
        self.assertNotEqual(obj.encoded_data, None)
        self.assertEqual(obj.content_type, "text/x-python")

    def test_store_binary_object_from_file_should_use_default_mimetype(self):
        bucket = self.client.bucket(self.bucket_name)
        filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                os.pardir, os.pardir, 'THANKS')
        obj = bucket.new_from_file(self.key_name, filepath)
        obj.store()
        obj = bucket.get(self.key_name)
        self.assertEqual(obj.content_type, 'application/octet-stream')

    def test_store_binary_object_from_file_should_fail_if_file_not_found(self):
        bucket = self.client.bucket(self.bucket_name)
        with self.assertRaises(IOError):
            bucket.new_from_file(self.key_name, 'FILE_NOT_FOUND')
        obj = bucket.get(self.key_name)
        # self.assertEqual(obj.encoded_data, None)
        self.assertFalse(obj.exists)


class CounterTests(object):
    def test_counter_requires_allow_mult(self):
        bucket = self.client.bucket(self.bucket_name)
        self.assertFalse(bucket.allow_mult)

        with self.assertRaises(Exception):
            bucket.update_counter(self.key_name, 10)

    def test_counter_ops(self):
        bucket = self.client.bucket(self.sibs_bucket)
        self.assertTrue(bucket.allow_mult)

        # Non-existent counter has no value
        self.assertEqual(None, bucket.get_counter(self.key_name))

        # Update the counter
        bucket.update_counter(self.key_name, 10)
        self.assertEqual(10, bucket.get_counter(self.key_name))

        # Update with returning the value
        self.assertEqual(15, bucket.update_counter(self.key_name, 5,
                                                   returnvalue=True))

        # Now try decrementing
        self.assertEqual(10, bucket.update_counter(self.key_name, -5,
                                                   returnvalue=True))
