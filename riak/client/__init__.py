try:
    import simplejson as json
except ImportError:
    import json

import random

from weakref import WeakValueDictionary
from riak.client.operations import RiakClientOperations
from riak.node import RiakNode
from riak.bucket import RiakBucket, BucketType
from riak.mapreduce import RiakMapReduceChain
from riak.resolver import default_resolver
from riak.table import Table
from riak.transports.http import HttpPool
from riak.transports.tcp import TcpPool
from riak.security import SecurityCreds
from riak.util import lazy_property, bytes_to_str, str_to_bytes
from six import string_types, PY2
from riak.client.multi import MultiGetPool, MultiPutPool


def default_encoder(obj):
    """
    Default encoder for JSON datatypes, which returns UTF-8 encoded
    json instead of the default bloated backslash u XXXX escaped ASCII strings.
    """
    if isinstance(obj, bytes):
        return json.dumps(bytes_to_str(obj),
                          ensure_ascii=False).encode("utf-8")
    else:
        return json.dumps(obj, ensure_ascii=False).encode("utf-8")


def binary_json_encoder(obj):
    """
    Default encoder for JSON datatypes, which returns UTF-8 encoded
    json instead of the default bloated backslash u XXXX escaped ASCII strings.
    """
    if isinstance(obj, bytes):
        return json.dumps(bytes_to_str(obj),
                          ensure_ascii=False).encode("utf-8")
    else:
        return json.dumps(obj, ensure_ascii=False).encode("utf-8")


def binary_json_decoder(obj):
    """
    Default decoder from JSON datatypes.
    """
    return json.loads(bytes_to_str(obj))


def binary_encoder_decoder(obj):
    """
    Assumes value is already in binary format, so passes unchanged.
    """
    return obj


class RiakClient(RiakMapReduceChain, RiakClientOperations):
    """
    The ``RiakClient`` object holds information necessary to connect
    to Riak. Requests can be made to Riak directly through the client
    or by using the methods on related objects.
    """

    #: The supported protocols
    PROTOCOLS = ['http', 'pbc']

    def __init__(self, protocol='pbc', transport_options={},
                 nodes=None, credentials=None,
                 multiget_pool_size=None, multiput_pool_size=None,
                 **kwargs):
        """
        Construct a new ``RiakClient`` object.

        :param protocol: the preferred protocol, defaults to 'pbc'
        :type protocol: string
        :param nodes: a list of node configurations,
           where each configuration is a dict containing the keys
           'host', 'http_port', and 'pb_port'
        :type nodes: list
        :param transport_options: Optional key-value args to pass to
                                  the transport constructor
        :type transport_options: dict
        :param credentials: optional object of security info
        :type credentials: :class:`~riak.security.SecurityCreds` or dict
        :param multiget_pool_size: the number of threads to use in
           :meth:`multiget` operations. Defaults to a factor of the number of
           CPUs in the system
        :type multiget_pool_size: int
        :param multiput_pool_size: the number of threads to use in
           :meth:`multiput` operations. Defaults to a factor of the number of
           CPUs in the system
        :type multiput_pool_size: int
        """
        kwargs = kwargs.copy()

        if nodes is None:
            self.nodes = [self._create_node(kwargs), ]
        else:
            self.nodes = [self._create_node(n) for n in nodes]

        self._multiget_pool_size = multiget_pool_size
        self._multiput_pool_size = multiput_pool_size
        self.protocol = protocol or 'pbc'
        self._resolver = None
        self._credentials = self._create_credentials(credentials)
        self._http_pool = HttpPool(self, **transport_options)
        self._tcp_pool = TcpPool(self, **transport_options)
        self._closed = False

        if PY2:
            self._encoders = {'application/json': default_encoder,
                              'text/json': default_encoder,
                              'text/plain': str}
            self._decoders = {'application/json': json.loads,
                              'text/json': json.loads,
                              'text/plain': str}
        else:
            self._encoders = {'application/json': binary_json_encoder,
                              'text/json': binary_json_encoder,
                              'text/plain': str_to_bytes,
                              'binary/octet-stream': binary_encoder_decoder}
            self._decoders = {'application/json': binary_json_decoder,
                              'text/json': binary_json_decoder,
                              'text/plain': bytes_to_str,
                              'binary/octet-stream': binary_encoder_decoder}
        self._buckets = WeakValueDictionary()
        self._bucket_types = WeakValueDictionary()
        self._tables = WeakValueDictionary()

    def __del__(self):
        self.close()

    def _get_protocol(self):
        return self._protocol

    def _set_protocol(self, value):
        if value not in self.PROTOCOLS:
            raise ValueError("protocol option is invalid, must be one of %s" %
                             repr(self.PROTOCOLS))
        self._protocol = value

    protocol = property(_get_protocol, _set_protocol,
                        doc="""
                        Which protocol to prefer, one of
                        :attr:`PROTOCOLS
                        <riak.client.RiakClient.PROTOCOLS>`. Please
                        note that when one protocol is selected, the
                        other protocols MAY NOT attempt to connect.
                        Changing to another protocol will cause a
                        connection on the next request.

                        Some requests are only valid over ``'http'``,
                        and will always be sent via
                        those transports, regardless of which protocol
                        is preferred.
                         """)

    def _get_resolver(self):
        return self._resolver or default_resolver

    def _set_resolver(self, value):
        if value is None or callable(value):
            self._resolver = value
        else:
            raise TypeError("resolver is not a function")

    resolver = property(_get_resolver, _set_resolver,
                        doc=""" The sibling-resolution function for this client.
                        Defaults to :func:`riak.resolver.default_resolver`.""")

    def _get_client_id(self):
        with self._transport() as transport:
            return transport.client_id

    def _set_client_id(self, client_id):
        for http in self._http_pool:
            http.client_id = client_id
        for pb in self._tcp_pool:
            pb.client_id = client_id

    client_id = property(_get_client_id, _set_client_id,
                         doc="""The client ID for this client instance""")

    def get_encoder(self, content_type):
        """
        Get the encoding function for the provided content type.

        :param content_type: the requested media type
        :type content_type: str
        :rtype: function
        """
        return self._encoders.get(content_type)

    def set_encoder(self, content_type, encoder):
        """
        Set the encoding function for the provided content type.

        :param content_type: the requested media type
        :type content_type: str
        :param encoder: an encoding function, takes a single object
            argument and returns encoded data
        :type encoder: function
        """
        self._encoders[content_type] = encoder

    def get_decoder(self, content_type):
        """
        Get the decoding function for the provided content type.

        :param content_type: the requested media type
        :type content_type: str
        :rtype: function
        """
        return self._decoders.get(content_type)

    def set_decoder(self, content_type, decoder):
        """
        Set the decoding function for the provided content type.

        :param content_type: the requested media type
        :type content_type: str
        :param decoder: a decoding function, takes encoded data and
            returns a Python type
        :type decoder: function
        """
        self._decoders[content_type] = decoder

    def bucket(self, name, bucket_type='default'):
        """
        Get the bucket by the specified name. Since buckets always exist,
        this will always return a
        :class:`RiakBucket <riak.bucket.RiakBucket>`.

        If you are using a bucket that is contained in a bucket type, it is
        preferable to access it from the bucket type object::

            # Preferred:
            client.bucket_type("foo").bucket("bar")

            # Equivalent, but not preferred:
            client.bucket("bar", bucket_type="foo")

        :param name: the bucket name
        :type name: str
        :param bucket_type: the parent bucket-type
        :type bucket_type: :class:`BucketType <riak.bucket.BucketType>`
              or str
        :rtype: :class:`RiakBucket <riak.bucket.RiakBucket>`

        """
        if not isinstance(name, string_types):
            raise TypeError('Bucket name must be a string')

        if isinstance(bucket_type, string_types):
            bucket_type = self.bucket_type(bucket_type)
        elif not isinstance(bucket_type, BucketType):
            raise TypeError('bucket_type must be a string '
                            'or riak.bucket.BucketType')

        return self._buckets.setdefault((bucket_type, name),
                                        RiakBucket(self, name, bucket_type))

    def bucket_type(self, name):
        """
        Gets the bucket-type by the specified name. Bucket-types do
        not always exist (unlike buckets), but this will always return
        a :class:`BucketType <riak.bucket.BucketType>` object.

        :param name: the bucket-type name
        :type name: str
        :rtype: :class:`BucketType <riak.bucket.BucketType>`
        """
        if not isinstance(name, string_types):
            raise TypeError('BucketType name must be a string')

        if name in self._bucket_types:
            return self._bucket_types[name]
        else:
            btype = BucketType(self, name)
            self._bucket_types[name] = btype
            return btype

    def table(self, name):
        """
        Gets the table by the specified name. Tables do
        not always exist (unlike buckets), but this will always return
        a :class:`Table <riak.table.Table>` object.

        :param name: the table name
        :type name: str
        :rtype: :class:`Table <riak.table.Table>`
        """
        if not isinstance(name, string_types):
            raise TypeError('Table name must be a string')

        if name in self._tables:
            return self._tables[name]
        else:
            table = Table(self, name)
            self._tables[name] = table
            return table

    def close(self):
        """
        Iterate through all of the connections and close each one.
        """
        if not self._closed:
            self._closed = True
            self._stop_multi_pools()
            if self._http_pool is not None:
                self._http_pool.clear()
                self._http_pool = None
            if self._tcp_pool is not None:
                self._tcp_pool.clear()
                self._tcp_pool = None

    def _stop_multi_pools(self):
        if self._multiget_pool:
            self._multiget_pool.stop()
            self._multiget_pool = None
        if self._multiput_pool:
            self._multiput_pool.stop()
            self._multiput_pool = None

    def _create_node(self, n):
        if isinstance(n, RiakNode):
            return n
        elif isinstance(n, tuple) and len(n) is 3:
            host, http_port, pb_port = n
            return RiakNode(host=host,
                            http_port=http_port,
                            pb_port=pb_port)
        elif isinstance(n, dict):
            return RiakNode(**n)
        else:
            raise TypeError("%s is not a valid node configuration"
                            % repr(n))

    def _create_credentials(self, n):
        """
        Create security credentials, if necessary.
        """
        if not n:
            return n
        elif isinstance(n, SecurityCreds):
            return n
        elif isinstance(n, dict):
            return SecurityCreds(**n)
        else:
            raise TypeError("%s is not a valid security configuration"
                            % repr(n))

    def _choose_node(self, nodes=None):
        """
        Chooses a random node from the list of nodes in the client,
        taking into account each node's recent error rate.
        :rtype RiakNode
        """
        if not nodes:
            nodes = self.nodes

        # Prefer nodes which have gone a reasonable time without
        # errors
        def _error_rate(node):
            return node.error_rate.value()

        good = [n for n in nodes if _error_rate(n) < 0.1]

        if len(good) is 0:
            # Fall back to a minimally broken node
            return min(nodes, key=_error_rate)
        else:
            return random.choice(good)

    @lazy_property
    def _multiget_pool(self):
        if self._multiget_pool_size:
            return MultiGetPool(self._multiget_pool_size)
        else:
            return None

    @lazy_property
    def _multiput_pool(self):
        if self._multiput_pool_size:
            return MultiPutPool(self._multiput_pool_size)
        else:
            return None

    def __hash__(self):
        return hash(frozenset([(n.host, n.http_port, n.pb_port)
                               for n in self.nodes]))

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
