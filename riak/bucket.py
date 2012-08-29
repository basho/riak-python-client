"""
Copyright 2010 Rusty Klophaus <rusty@basho.com>
Copyright 2010 Justin Sheehy <justin@basho.com>
Copyright 2009 Jay Baird <jay@mochimedia.com>

This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""
from riak_object import RiakObject
import mimetypes


class RiakBucket(object):
    """
    The ``RiakBucket`` object allows you to access and change information
    about a Riak bucket, and provides methods to create or retrieve
    objects within the bucket.
    """

    SEARCH_PRECOMMIT_HOOK = {"mod": "riak_search_kv_hook", "fun": "precommit"}

    def __init__(self, client, name):
        """
        Returns a new ``RiakBucket`` instance.

        :param client: A :class:`RiakClient <riak.client.RiakClient>` instance
        :type client: :class:`RiakClient <riak.client.RiakClient>`
        :param name: The bucket name
        :type name: string
        """
        try:
            if isinstance(name, basestring):
                name = name.encode('ascii')
        except UnicodeError:
            raise TypeError('Unicode bucket names are not supported.')

        self._client = client
        self._name = name
        self._r = None
        self._w = None
        self._dw = None
        self._rw = None
        self._pr = None
        self._pw = None
        self._encoders = {}
        self._decoders = {}

    def get_name(self):
        """
        Get the bucket name as a string.
        """
        return self._name

    def get_r(self, r=None):
        """
        Get the R-value for this bucket, if it is set, otherwise return
        the R-value for the client.

        :rtype: integer
        """
        if (r is not None):
            return r
        if (self._r is not None):
            return self._r
        return self._client.get_r()

    def set_r(self, r):
        """
        Set the R-value for this bucket. This value is used by :func:`get`
        and :func:`get_binary` operations that do not specify an R-value.

        :param r: The new R-value.
        :type r: integer
        :rtype: self
        """
        self._r = r
        return self

    def get_w(self, w=None):
        """
        Get the W-value for this bucket, if it is set, otherwise return
        the W-value for the client.

        :rtype: integer
        """
        if (w is not None):
            return w
        if (self._w is not None):
            return self._w
        return self._client.get_w()

    def set_w(self, w):
        """
        Set the W-value for this bucket. See :func:`set_r` for
        more information.

        :param w: The new W-value.
        :type w: integer
        :rtype: self
        """
        self._w = w
        return self

    def get_dw(self, dw=None):
        """
        Get the DW-value for this bucket, if it is set, otherwise return
        the DW-value for the client.

        :rtype: integer
        """
        if (dw is not None):
            return dw
        if (self._dw is not None):
            return self._dw
        return self._client.get_dw()

    def set_dw(self, dw):
        """
        Set the DW-value for this bucket. See :func:`set_r` for more
        information.

        :param dw: The new DW-value
        :type dw: integer
        :rtype: self
        """
        self._dw = dw
        return self

    def get_rw(self, rw=None):
        """
        Get the RW-value for this bucket, if it is set, otherwise return
        the RW-value for the client.

        :rtype: integer
        """
        if (rw is not None):
            return rw
        if (self._rw is not None):
            return self._rw
        return self._client.get_rw()

    def set_rw(self, rw):
        """
        Set the RW-value for this bucket. See :func:`set_r` for more
        information.

        :param rw: The new RW-value
        :type rw: integer
        :rtype: self
        """
        self._rw = rw
        return self

    def get_pr(self, pr=None):
        """
        Get the PR-value for this bucket, if it is set, otherwise return
        the PR-value for the client.

        :rtype: integer
        """
        if (pr is not None):
            return pr
        if (self._pr is not None):
            return self._pr
        return self._client.get_pr()

    def set_pr(self, pr):
        """
        Set the PR-value for this bucket. See :func:`set_r` for more
        information.

        :param pr: The new PR-value
        :type pr: integer
        :rtype: self
        """
        self._pr = pr
        return self

    def get_pw(self, pw=None):
        """
        Get the PW-value for this bucket, if it is set, otherwise return
        the PW-value for the client.

        :rtype: integer
        """
        if (pw is not None):
            return pw
        if (self._pw is not None):
            return self._pw
        return self._client.get_pw()

    def set_pw(self, pw):
        """
        Set the PW-value for this bucket. See :func:`set_r` for more
        information.

        :param pw: The new PR-value
        :type pw: integer
        :rtype: self
        """
        self._pw = pw
        return self

    def get_encoder(self, content_type):
        """
        Get the encoding function for the provided content type for
        this bucket.

        :param content_type: Content type requested
        """
        if content_type in self._encoders:
            return self._encoders[content_type]
        else:
            return self._client.get_encoder(content_type)

    def set_encoder(self, content_type, encoder):
        """
        Set the encoding function for the provided content type for
        this bucket.

        :param content_type: Content type for encoder
        :param encoder: Function to encode with - will be called with
                        data as single argument.
        """
        self._encoders[content_type] = encoder
        return self

    def get_decoder(self, content_type):
        """
        Get the decoding function for the provided content type for
        this bucket.

        :param content_type: Content type for decoder
        """
        if content_type in self._decoders:
            return self._decoders[content_type]
        else:
            return self._client.get_decoder(content_type)

    def set_decoder(self, content_type, decoder):
        """
        Set the decoding function for the provided content type for
        this bucket.

        :param content_type: Content type for decoder
        :param decoder: Function to decode with - will be called with
                        string
        """
        self._decoders[content_type] = decoder
        return self

    def new(self, key=None, data=None, content_type='application/json'):
        """
        Create a new :class:`RiakObject <riak.riak_object.RiakObject>`
        that will be stored as JSON. A shortcut for manually
        instantiating a :class:`RiakObject
        <riak.riak_object.RiakObject>`.

        :param key: Name of the key. Leaving this to be None (default)
                    will make Riak generate the key on store.
        :type key: string
        :param data: The data to store.
        :type data: object
        :rtype: :class:`RiakObject <riak.riak_object.RiakObject>`
        """
        try:
            if isinstance(data, basestring):
                data = data.encode('ascii')
        except UnicodeError:
            raise TypeError('Unicode data values are not supported.')

        obj = RiakObject(self._client, self, key)
        obj.set_data(data)
        obj.set_content_type(content_type)
        obj._encode_data = True
        return obj

    def new_binary(self, key, data, content_type='application/octet-stream'):
        """
        Create a new :class:`RiakObject <riak.riak_object.RiakObject>`
        that will be stored as plain text/binary. A shortcut for
        manually instantiating a :class:`RiakObject
        <riak.riak_object.RiakObject>`.

        :param key: Name of the key.
        :type key: string
        :param data: The data to store.
        :type data: object
        :param content_type: The content type of the object.
        :type content_type: string
        :rtype: :class:`RiakObject <riak.riak_object.RiakObject>`
        """
        obj = RiakObject(self._client, self, key)
        obj.set_data(data)
        obj.set_content_type(content_type)
        obj._encode_data = False
        return obj

    def get(self, key, r=None, pr=None):
        """
        Retrieve a JSON-encoded object from Riak.

        :param key: Name of the key.
        :type key: string
        :param r: R-Value of the request (defaults to bucket's R)
        :type r: integer
        :param pr: PR-Value of the request (defaults to bucket's PR)
        :type pr: integer
        :rtype: :class:`RiakObject <riak.riak_object.RiakObject>`
        """
        obj = RiakObject(self._client, self, key)
        obj._encode_data = True
        r = self.get_r(r)
        pr = self.get_pr(pr)
        return obj.reload(r=r, pr=pr)

    def get_binary(self, key, r=None, pr=None):
        """
        Retrieve a binary/string object from Riak.

        :param key: Name of the key.
        :type key: string
        :param r: R-Value of the request (defaults to bucket's R)
        :type r: integer
        :param pr: PR-Value of the request (defaults to bucket's PR)
        :type pr: integer
        :rtype: :class:`RiakObject <riak.riak_object.RiakObject>`
        """
        obj = RiakObject(self._client, self, key)
        obj._encode_data = False
        r = self.get_r(r)
        pr = self.get_pr(pr)
        return obj.reload(r=r, pr=pr)

    def set_n_val(self, nval):
        """
        Set the N-value for this bucket, which is the number of replicas
        that will be written of each object in the bucket.

        .. warning::

           Set this once before you write any data to the bucket, and never
           change it again, otherwise unpredictable things could happen.
           This should only be used if you know what you are doing.

        :param nval: The new N-Val.
        :type nval: integer
        """
        return self.set_property('n_val', nval)

    def get_n_val(self):
        """
        Retrieve the N-value for this bucket.

        :rtype: integer
        """
        return self.get_property('n_val')

    def set_default_r_val(self, rval):
        return self.set_property('r', rval)

    def get_default_r_val(self):
        return self.get_property('r')

    def set_default_w_val(self, wval):
        return self.set_property('w', wval)

    def get_default_w_val(self):
        return self.get_property('w')

    def set_default_dw_val(self, dwval):
        return self.set_property('dw', dwval)

    def get_default_dw_val(self):
        return self.get_property('dw')

    def set_default_rw_val(self, rwval):
        return self.set_property('rw', rwval)

    def get_default_rw_val(self):
        return self.get_property('rw')

    def set_allow_multiples(self, bool):
        """
        If set to True, then writes with conflicting data will be stored
        and returned to the client. This situation can be detected by
        calling has_siblings() and get_siblings().

        :param bool: True to store and return conflicting writes.
        :type bool: boolean
        """
        return self.set_property('allow_mult', bool)

    def get_allow_multiples(self):
        """
        Retrieve the 'allow multiples' setting.

        :rtype: Boolean
        """
        return self.get_bool_property('allow_mult')

    def set_property(self, key, value):
        """
        Set a bucket property.

        .. warning::

           This should only be used if you know what you are doing.

        :param key: Property to set.
        :type key: string
        :param value: Property value.
        :type value: mixed
        """
        return self.set_properties({key: value})

    def get_bool_property(self, key):
        """
        Get a boolean bucket property. Converts to a ``True`` or
        ``False`` value.

        :param key: Property to set.
        :type key: string
        """
        prop = self.get_property(key)
        if prop == True or prop > 0:
            return True
        else:
            return False

    def get_property(self, key):
        """
        Retrieve a bucket property.

        :param key: The property to retrieve.
        :type key: string
        :rtype: mixed
        """
        props = self.get_properties()
        if (key in props.keys()):
            return props[key]

    def set_properties(self, props):
        """
        Set multiple bucket properties in one call.

        .. warning::

           This should only be used if you know what you are doing.

        :param props: An associative array of key:value.
        :type props: array
        """
        t = self._client.get_transport()
        t.set_bucket_props(self, props)

    def get_properties(self):
        """
        Retrieve an associative array of all bucket properties.

        :rtype: array
        """
        t = self._client.get_transport()
        return t.get_bucket_props(self)

    def get_keys(self):
        """
        Return all keys within the bucket.

        .. warning::

           At current, this is a very expensive operation. Use with caution.
        """
        return self._client.get_transport().get_keys(self)

    def new_binary_from_file(self, key, filename):
        """
        Create a new Riak object in the bucket, using the content of
        the specified file.
        """
        binary_data = open(filename, "rb").read()
        mimetype, encoding = mimetypes.guess_type(filename)
        if not mimetype:
            mimetype = 'application/octet-stream'
        return self.new_binary(key, binary_data, mimetype)

    def search_enabled(self):
        """
        Returns True if the search precommit hook is enabled for this
        bucket.
        """
        return self.SEARCH_PRECOMMIT_HOOK in (self.get_property("precommit") or
                                              [])

    def enable_search(self):
        """
        Enable search for this bucket by installing the precommit hook to
        index objects in it.
        """
        precommit_hooks = self.get_property("precommit") or []
        if self.SEARCH_PRECOMMIT_HOOK not in precommit_hooks:
            self.set_properties({"precommit":
                precommit_hooks + [self.SEARCH_PRECOMMIT_HOOK]})
        return True

    def disable_search(self):
        """
        Disable search for this bucket by removing the precommit hook to
        index objects in it.
        """
        precommit_hooks = self.get_property("precommit") or []
        if self.SEARCH_PRECOMMIT_HOOK in precommit_hooks:
            precommit_hooks.remove(self.SEARCH_PRECOMMIT_HOOK)
            self.set_properties({"precommit": precommit_hooks})
        return True

    def search(self, query, **params):
        """
        Queries a search index over objects in this bucket/index.
        """
        return self._client.solr().search(self._name, query, **params)

    def get_index(self, index, startkey, endkey=None):
        """
        Queries a secondary index over objects in this bucket, returning keys.
        """
        return self._client._transport.get_index(self._name, index, startkey,
                                                 endkey)
