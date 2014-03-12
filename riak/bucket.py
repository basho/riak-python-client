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

    def __init__(self, client, name, bucket_type):
        """
        Returns a new ``RiakBucket`` instance.

        :param client: A :class:`RiakClient <riak.client.RiakClient>` instance
        :type client: :class:`RiakClient <riak.client.RiakClient>`
        :param name: The bucket name
        :type name: string
        :param bucket_type: The parent bucket type of this bucket
        :type bucket_type: :class:`BucketType`
        """
        try:
            if isinstance(name, basestring):
                name = name.encode('ascii')
            else:
                raise TypeError('Bucket name must be a string')
        except UnicodeError:
            raise TypeError('Unicode bucket names are not supported.')

        self._client = client
        self.name = name
        self.bucket_type = bucket_type
        self._encoders = {}
        self._decoders = {}
        self._resolver = None

    def __hash__(self):
        return hash((self.bucket_type.name, self.name, self._client))

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

        :param content_type: the requested media type
        :type content_type: str
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

        :param content_type: the requested media type
        :type content_type: str
        :param encoder: an encoding function, takes a single object
            argument and returns a string data as single argument.
        :type encoder: function
        """
        self._encoders[content_type] = encoder
        return self

    def get_decoder(self, content_type):
        """
        Get the decoding function for the provided content type for
        this bucket.

        :param content_type: the requested media type
        :type content_type: str
        :rtype: function
        """
        if content_type in self._decoders:
            return self._decoders[content_type]
        else:
            return self._client.get_decoder(content_type)

    def set_decoder(self, content_type, decoder):
        """
        Set the decoding function for the provided content type for
        this bucket.

        :param content_type: the requested media type
        :type content_type: str
        :param decoder: a decoding function, takes a string and
            returns a Python type
        :type decoder: function
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

        .. deprecated:: 2.0.0
           Use :meth:`new` instead.

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

    def get(self, key, r=None, pr=None, timeout=None):
        """
        Retrieve an object from Riak.

        :param key: Name of the key.
        :type key: string
        :param r: R-Value of the request (defaults to bucket's R)
        :type r: integer
        :param pr: PR-Value of the request (defaults to bucket's PR)
        :type pr: integer
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :rtype: :class:`RiakObject <riak.riak_object.RiakObject>`
        """
        obj = RiakObject(self._client, self, key)
        return obj.reload(r=r, pr=pr, timeout=timeout)

    def get_binary(self, key, r=None, pr=None, timeout=None):
        """
        Retrieve a binary/string object from Riak.

        .. deprecated:: 2.0.0
           Use :meth:`get` instead.

        :param key: Name of the key.
        :type key: string
        :param r: R-Value of the request (defaults to bucket's R)
        :type r: integer
        :param pr: PR-Value of the request (defaults to bucket's PR)
        :type pr: integer
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :rtype: :class:`RiakObject <riak.riak_object.RiakObject>`
        """
        deprecated('RiakBucket.get_binary is deprecated, '
                   'use RiakBucket.get')
        return self.get(key, r=r, pr=pr, timeout=timeout)

    def multiget(self, keys, r=None, pr=None):
        """
        Retrieves a list of keys belonging to this bucket in parallel.

        :param keys: the keys to fetch
        :type keys: list
        :param r: R-Value for the requests (defaults to bucket's R)
        :type r: integer
        :param pr: PR-Value for the requests (defaults to bucket's PR)
        :type pr: integer
        :rtype: list of :class:`RiakObject <riak.riak_object.RiakObject>`
        """
        bkeys = [(self.name, key) for key in keys]
        return self._client.multiget(bkeys, r=r, pr=pr)

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
                           client's resolver will be used.""")

    n_val = bucket_property('n_val', doc="""
    N-value for this bucket, which is the number of replicas
    that will be written of each object in the bucket.

    .. warning:: Set this once before you write any data to the
        bucket, and never change it again, otherwise unpredictable
        things could happen. This should only be used if you know what
        you are doing.
    """)

    allow_mult = bucket_property('allow_mult', doc="""
    If set to True, then writes with conflicting data will be stored
    and returned to the client.

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

        :rtype: list of keys
        """
        return self._client.get_keys(self)

    def stream_keys(self):
        """
        Streams all keys within the bucket through an iterator.

        :rtype: iterator
        """
        return self._client.stream_keys(self)

    def new_from_file(self, key, filename):
        """
        Create a new Riak object in the bucket, using the contents of
        the specified file. This is a shortcut for :meth:`new`, where the
        ``encoded_data`` and ``content_type`` are set for you.

        :param key: the key of the new object
        :type key: string
        :param filename: the file to read the contents from
        :type filename: string
        :rtype: :class:`RiakObject <riak.riak_object.RiakObject>`
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
        """
        Create a new Riak object in the bucket, using the contents of
        the specified file. This is a shortcut for :meth:`new`, where the
        ``encoded_data`` and ``content_type`` are set for you.

        .. deprecated:: 2.0.0
           Use :meth:`new_from_file` instead.

        :param key: the key of the new object
        :type key: string
        :param filename: the file to read the contents from
        :type filename: string
        :rtype: :class:`RiakObject <riak.riak_object.RiakObject>`
        """
        deprecated('RiakBucket.new_binary_from_file is deprecated, use '
                   'RiakBucket.new_from_file')
        return self.new_from_file(key, filename)

    def search_enabled(self):
        """
        Returns True if search indexing is enabled for this
        bucket.
        """
        return self.get_properties().get('search', False)

    def enable_search(self):
        """
        Enable search indexing for this bucket.
        """
        if not self.search_enabled():
            self.set_property('search', True)
        return True

    def disable_search(self):
        """
        Disable search indexing for this bucket.
        """
        if self.search_enabled():
            self.set_property('search', False)
        return True

    def search(self, query, **params):
        """
        Queries a search index over objects in this bucket/index. See
        :meth:`RiakClient.fulltext_search()
        <riak.client.RiakClient.fulltext_search>` for more details.
        """
        return self._client.fulltext_search(self.name, query, **params)

    def get_index(self, index, startkey, endkey=None, return_terms=None,
                  max_results=None, continuation=None, timeout=None,
                  term_regex=None):
        """
        Queries a secondary index over objects in this bucket,
        returning keys or index/key pairs. See
        :meth:`RiakClient.get_index()
        <riak.client.RiakClient.get_index>` for more details.
        """
        return self._client.get_index(self, index, startkey, endkey,
                                      return_terms=return_terms,
                                      max_results=max_results,
                                      continuation=continuation,
                                      timeout=timeout, term_regex=term_regex)

    def stream_index(self, index, startkey, endkey=None, return_terms=None,
                     max_results=None, continuation=None, timeout=None,
                     term_regex=None):
        """
        Queries a secondary index over objects in this bucket,
        streaming keys or index/key pairs via an iterator. See
        :meth:`RiakClient.stream_index()
        <riak.client.RiakClient.stream_index>` for more details.
        """
        return self._client.stream_index(self, index, startkey, endkey,
                                         return_terms=return_terms,
                                         max_results=max_results,
                                         continuation=continuation,
                                         timeout=timeout,
                                         term_regex=term_regex)

    def delete(self, key, **kwargs):
        """Deletes an object from riak. Short hand for
        bucket.new(key).delete(). See :meth:`RiakClient.delete()
        <riak.client.RiakClient.delete>` for options.

        :param key: The key for the object
        :type key: string
        :rtype: RiakObject
        """
        return self.new(key).delete(**kwargs)

    def get_counter(self, key, **kwargs):
        """
        Gets the value of a counter stored in this bucket. See
        :meth:`RiakClient.get_counter()
        <riak.client.RiakClient.get_counter>` for options.

        :param key: the key of the counter
        :type key: string
        :rtype: int
        """
        return self._client.get_counter(self, key, **kwargs)

    def update_counter(self, key, value, **kwargs):
        """
        Updates the value of a counter stored in this bucket. Positive
        values increment the counter, negative values decrement. See
        :meth:`RiakClient.update_counter()
        <riak.client.RiakClient.update_counter>` for options.


        :param key: the key of the counter
        :type key: string
        :param value: the amount to increment or decrement
        :type value: integer
        """
        return self._client.update_counter(self, key, value, **kwargs)

    increment_counter = update_counter

    def __str__(self):
        if self.bucket_type.is_default():
            return '<RiakBucket {0!r}>'.format(self.name)
        else:
            return '<RiakBucket {0!r}/{1!r}>'.format(self.bucket_type.name,
                                                     self.name)

    __repr__ = __str__


class BucketType(object):
    """
    The ``BucketType`` object allows you to access and change
    properties on a Riak bucket type and access buckets within its
    namespace.
    """
    def __init__(self, client, name):
        """
        Returns a new ``BucketType`` instance.

        :param client: A :class:`RiakClient <riak.client.RiakClient>` instance
        :type client: :class:`RiakClient <riak.client.RiakClient>`
        :param name: The bucket-type name
        :type name: string
        """
        self._client = client
        self.name = name

    def is_default(self):
        """
        Whether this bucket type is the default type, or a
        user-defined type.

        :rtype: bool
        """
        return self.name == 'default'

    def get_property(self, key, value):
        """
        Retrieve a bucket-type property.

        :param key: The property to retrieve.
        :type key: string
        :rtype: mixed
        """
        return self.get_properties()[key]

    def set_property(self, key, value):
        """
        Set a bucket-type property.

        :param key: Property to set.
        :type key: string
        :param value: Property value.
        :type value: mixed
        """
        return self.set_properties({key: value})

    def get_properties(self):
        """
        Retrieve a dict of all bucket-type properties.

        :rtype: dict
        """
        return self._client.get_bucket_type_props(self)

    def set_properties(self, props):
        """
        Set multiple bucket-type properties in one call.

        :param props: A dictionary of properties
        :type props: dict
        """
        self._client.set_bucket_type_props(self, props)

    def bucket(self, name):
        """
        Gets a bucket that belongs to this bucket-type.

        :param name: the bucket name
        :type name: str
        :rtype: :class:`RiakBucket`
        """
        return self._client.bucket(name, self)

    def get_buckets(self, timeout=None):
        """
        Get the list of buckets under this bucket-type as
        :class:`RiakBucket <riak.bucket.RiakBucket>` instances.

        .. warning:: Do not use this in production, as it requires
           traversing through all keys stored in a cluster.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :rtype: list of :class:`RiakBucket <riak.bucket.RiakBucket>` instances
        """
        return self._client.get_buckets(bucket_type=self, timeout=timeout)

    def stream_buckets(self, timeout=None):
        """
        Streams the list of buckets under this bucket-type. This is a
        generator method that should be iterated over.

        .. warning:: Do not use this in production, as it requires
           traversing through all keys stored in a cluster.

        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :rtype: iterator that yields lists of :class:`RiakBucket
             <riak.bucket.RiakBucket>` instances
        """
        return self._client.stream_buckets(bucket_type=self, timeout=timeout)

    def __str__(self):
        return "<BucketType {0!r}>".format(self.name)

    __repr__ = __str__


from riak_object import RiakObject
