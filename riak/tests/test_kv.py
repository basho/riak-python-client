# -*- coding: utf-8 -*-
import os
import cPickle
import copy
try:
    import simplejson as json
except ImportError:
    import json


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

    def test_store_metadata(self):
        bucket = self.client.bucket('bucket')
        rand = self.randint()
        obj = bucket.new('fooster', rand)
        obj.set_usermeta({'custom': 'some metadata'})
        obj.store()
        obj = bucket.get('fooster')
        self.assertEqual('some metadata', obj.get_usermeta()['custom'])

    def test_list_buckets(self):
        bucket = self.client.bucket("list_bucket")
        bucket.new("one", {"foo": "one", "bar": "red"}).store()
        buckets = self.client.get_buckets()
        self.assertTrue("list_bucket" in buckets)


class KVFileTests(object):
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

    def test_store_binary_object_from_file_should_fail_if_file_not_found(self):
        bucket = self.client.bucket('bucket')
        rand = str(self.randint())
        self.assertRaises(IOError, bucket.new_binary_from_file,
                          'not_found_from_file', 'FILE_NOT_FOUND')
        obj = bucket.get_binary('not_found_from_file')
        self.assertEqual(obj.get_data(), None)
