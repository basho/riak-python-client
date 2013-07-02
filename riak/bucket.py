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
import mimetypes
from riak.util import deprecateQuorumAccessors, deprecated


def deprecateBucketQuorumAccessors(klass):
    return deprecateQuorumAccessors(klass, parent='_client')


def bucket_property(name, doc=None):
    def _prop_getter(self):
        return self.get_property(name)

    def _prop_setter(self, value):
        return self.set_property(name, value)

    return property(_prop_getter, _prop_setter, doc=doc)


@deprecateBucketQuorumAccessors
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
        self.name = name
        self._encoders = {}
        self._decoders = {}
        self._resolver = None

    def __hash__(self):
        return hash((self.name, self._client))

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return hash(self) == hash(other)
        else:
            return False

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return hash(self) != hash(other)
        else:
            return True

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

    def new(self, key=None, data=None, content_type='application/json',
            encoded_data=None):
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
        obj.content_type = content_type
        if data is not None:
            obj.data = data
        if encoded_data is not None:
            obj.encoded_data = encoded_data
        return obj

    def new_binary(self, key=None, data=None,
                   content_type='application/octet-stream'):
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
        deprecated('RiakBucket.new_binary is deprecated, '
                   'use RiakBucket.new with the encoded_data '
                   'param instead of data')
        return self.new(key, encoded_data=data, content_type=content_type)

    def get(self, key, r=None, pr=None):
        """
        Retrieve an object from Riak.

        :param key: Name of the key.
        :type key: string
        :param r: R-Value of the request (defaults to bucket's R)
        :type r: integer
        :param pr: PR-Value of the request (defaults to bucket's PR)
        :type pr: integer
        :rtype: :class:`RiakObject <riak.riak_object.RiakObject>`
        """
        obj = RiakObject(self._client, self, key)
        return obj.reload(r=r, pr=pr)

    def get_binary(self, key, r=None, pr=None):
        """
        Retrieve a binary/string object from Riak. DEPRECATED

        :param key: Name of the key.
        :type key: string
        :param r: R-Value of the request (defaults to bucket's R)
        :type r: integer
        :param pr: PR-Value of the request (defaults to bucket's PR)
        :type pr: integer
        :rtype: :class:`RiakObject <riak.riak_object.RiakObject>`
        """
        deprecated('RiakBucket.get_binary is deprecated, '
                   'use RiakBucket.get')
        return self.get(key, r=r, pr=pr)

    def _get_resolver(self):
        if callable(self._resolver):
            return self._resolver
        elif self._resolver is None:
            return self._client.resolver
        else:
            raise TypeError("resolver is not a function")

    def _set_resolver(self, value):
        if value is None or callable(value):
            self._resolver = value
        else:
            raise TypeError("resolver is not a function")

    resolver = property(_get_resolver, _set_resolver, doc=
                        """The sibling-resolution function for this
                           bucket. If the resolver is not set, the
                           client's resolver will be used. :type
                           callable""")

    n_val = bucket_property('n_val', doc="""
    N-value for this bucket, which is the number of replicas
    that will be written of each object in the bucket.

    .. warning::

    Set this once before you write any data to the bucket, and never
    change it again, otherwise unpredictable things could happen.
    This should only be used if you know what you are doing.

    :type nval: integer
    """)

    allow_mult = bucket_property('allow_mult', doc="""
    If set to True, then writes with conflicting data will be stored
    and returned to the client. This situation can be detected by
    calling has_siblings() and get_siblings().

    :type bool: boolean
    """)

    r = bucket_property('r', doc="""
    The default 'read' quorum for this bucket (how many replicas must
    reply for a successful read). This should be an integer less than
    the 'n_val' property, or a string of 'one', 'quorum', 'all', or
    'default'""")

    pr = bucket_property('pr', doc="""
    The default 'primary read' quorum for this bucket (how many
    primary replicas are required for a successful read). This should
    be an integer less than the 'n_val' property, or a string of
    'one', 'quorum', 'all', or 'default'""")

    rw = bucket_property('rw', doc="""
    The default 'read' and 'write' quorum for this bucket (equivalent
    to 'r' and 'w' but for deletes). This should be an integer less
    than the 'n_val' property, or a string of 'one', 'quorum', 'all',
    or 'default'""")

    w = bucket_property('w', doc="""
    The default 'write' quorum for this bucket (how many replicas must
    acknowledge receipt of a write). This should be an integer less
    than the 'n_val' property, or a string of 'one', 'quorum', 'all',
    or 'default'""")

    dw = bucket_property('dw', doc="""
    The default 'durable write' quorum for this bucket (how many
    replicas must commit the write). This should be an integer less
    than the 'n_val' property, or a string of 'one', 'quorum', 'all',
    or 'default'""")

    pw = bucket_property('pw', doc="""
    The default 'primary write' quorum for this bucket (how many
    primary replicas are required for a successful write). This should
    be an integer less than the 'n_val' property, or a string of
    'one', 'quorum', 'all', or 'default'""")

    def set_property(self, key, value):
        """
        Set a bucket property.

        :param key: Property to set.
        :type key: string
        :param value: Property value.
        :type value: mixed
        """
        return self.set_properties({key: value})

    def get_property(self, key):
        """
        Retrieve a bucket property.

        :param key: The property to retrieve.
        :type key: string
        :rtype: mixed
        """
        return self.get_properties()[key]

    def set_properties(self, props):
        """
        Set multiple bucket properties in one call.

        :param props: A dictionary of properties
        :type props: dict
        """
        self._client.set_bucket_props(self, props)

    def get_properties(self):
        """
        Retrieve a dict of all bucket properties.

        :rtype: dict
        """
        return self._client.get_bucket_props(self)

    def clear_properties(self):
        """
        Reset all bucket properties to their defaults.

        """
        return self._client.clear_bucket_props(self)

    def get_keys(self):
        """
        Return all keys within the bucket.

        .. warning::

           At current, this is a very expensive operation. Use with caution.
        """
        return self._client.get_keys(self)

    def stream_keys(self):
        """
        Streams all keys within the bucket through an iterator.

        .. warning::

           At current, this is a very expensive operation. Use with caution.

        :rtype: iterator
        """
        return self._client.stream_keys(self)

    def new_from_file(self, key, filename):
        """
        Create a new Riak object in the bucket, using the content of
        the specified file.
        """
        binary_data = open(filename, "rb").read()
        mimetype, encoding = mimetypes.guess_type(filename)
        if encoding:
            binary_data = bytearray(binary_data, encoding)
        else:
            binary_data = bytearray(binary_data)
        if not mimetype:
            mimetype = 'application/octet-stream'
        return self.new(key, encoded_data=binary_data, content_type=mimetype)

    def new_binary_from_file(self, key, filename):
        deprecated('RiakBucket.new_binary_from_file is deprecated, use '
                   'RiakBucket.new_from_file')
        return self.new_from_file(key, filename)

    def search_enabled(self):
        """
        Returns True if the search precommit hook is enabled for this
        bucket.
        """
        return self.get_properties().get('search', False)

    def enable_search(self):
        """
        Enable search for this bucket by installing the precommit hook to
        index objects in it.
        """
        if not self.search_enabled():
            self.set_property('search', True)
        return True

    def disable_search(self):
        """
        Disable search for this bucket by removing the precommit hook to
        index objects in it.
        """
        if self.search_enabled():
            self.set_property('search', False)
        return True

    def search(self, query, **params):
        """
        Queries a search index over objects in this bucket/index.
        """
        return self._client.solr.search(self.name, query, **params)

    def get_index(self, index, startkey, endkey=None, return_terms=None):
        """
        Queries a secondary index over objects in this bucket, returning keys.
        """
        return self._client.get_index(self.name, index, startkey, endkey,
                                      return_terms=return_terms)

    def stream_index(self, index, startkey, endkey=None, return_terms=None):
        """
        Queries a secondary index over objects in this bucket,
        streaming keys via an iterator.
        """
        return self._client.stream_index(self.name, index, startkey, endkey,
                                         return_terms=return_terms)

    def delete(self, key, **kwargs):
        """Deletes an object from riak.

        Short hand for bucket.new(key).delete()
        :param key: The key for the object
        :type key: string
        :rtype: RiakObject
        """
        return self.new(key).delete(**kwargs)

    def __str__(self):
        return '<RiakBucket "{0}">'.format(self.name)

from riak_object import RiakObject
