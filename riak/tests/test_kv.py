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

    def test_missing_object(self):
        bucket = self.client.bucket(self.bucket_name)
        obj = bucket.get(self.key_name)
        self.assertFalse(obj.exists)
        self.assertEqual(obj.data, None)

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

        # Store the same object five times...
        # First run through should overwrite the datum 'start' above
        other_client = self.create_client()
        other_bucket = other_client.bucket(self.sibs_bucket)

        vals = set()
        for i in range(5):
            while True:
                randval = self.randint()
                if str(randval) not in vals:
                    break

            other_obj = other_bucket.new(key=self.key_name,
                                         encoded_data=str(randval),
                                         content_type='text/plain')
            other_obj.vclock = obj.vclock
            other_obj.store()
            vals.add(str(randval))

        # Make sure the object has itself plus four siblings...
        obj = bucket.get(self.key_name)
        obj.reload()
        self.assertTrue(bool(obj.siblings))
        self.assertEqual(len(obj.siblings), 5)

        # Get each of the values - make sure they match what was assigned
        vals2 = set()
        for i in xrange(len(obj.siblings)):
            vals2.add(obj.get_sibling(i).encoded_data)
        self.assertEqual(vals, vals2)

        # Resolve the conflict, and then do a get...
        obj3 = obj.get_sibling(3)
        obj3.store()

        obj.reload()
        self.assertEqual(len(obj.siblings), 0)
        self.assertEqual(obj.encoded_data, obj3.encoded_data)

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


class HTTPBucketPropsTest(object):
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


class PbcBucketPropsTest(object):
    def test_rw_settings(self):
        bucket = self.client.bucket(self.props_bucket)
        with self.assertRaises(NotImplementedError):
            bucket.r
        with self.assertRaises(NotImplementedError):
            bucket.w
        with self.assertRaises(NotImplementedError):
            bucket.dw
        with self.assertRaises(NotImplementedError):
            bucket.rw

        with self.assertRaises(NotImplementedError):
            bucket.r = 2
        with self.assertRaises(NotImplementedError):
            bucket.w = 2
        with self.assertRaises(NotImplementedError):
            bucket.dw = 2
        with self.assertRaises(NotImplementedError):
            bucket.rw = 2
        with self.assertRaises(NotImplementedError):
            bucket.clear_properties()

    def test_primary_quora(self):
        bucket = self.client.bucket(self.props_bucket)
        with self.assertRaises(NotImplementedError):
            bucket.pr
        with self.assertRaises(NotImplementedError):
            bucket.pw

        with self.assertRaises(NotImplementedError):
            bucket.pr = 2
        with self.assertRaises(NotImplementedError):
            bucket.pw = 2


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
            bucket.new_from_file('not_found_from_file', 'FILE_NOT_FOUND')
        obj = bucket.get('not_found_from_file')
        self.assertEqual(obj.encoded_data, None)
        self.assertFalse(obj.exists)
