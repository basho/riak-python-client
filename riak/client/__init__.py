"""
Copyright 2011 Basho Technologies, Inc.
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

try:
    import simplejson as json
except ImportError:
    import json

import random
from weakref import WeakValueDictionary
from riak.client.operations import RiakClientOperations
from riak.node import RiakNode
from riak.bucket import RiakBucket
from riak.mapreduce import RiakMapReduceChain
from riak.resolver import default_resolver
from riak.search import RiakSearch
from riak.transports.http import RiakHttpPool
from riak.transports.pbc import RiakPbcPool
from riak.util import deprecated
from riak.util import deprecateQuorumAccessors
from riak.util import lazy_property


def default_encoder(obj):
    """
    Default encoder for JSON datatypes, which returns UTF-8 encoded
    json instead of the default bloated \uXXXX escaped ASCII strings.
    """
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


@deprecateQuorumAccessors
class RiakClient(RiakMapReduceChain, RiakClientOperations):
    """
    The ``RiakClient`` object holds information necessary to connect
    to Riak. Requests can be made to Riak directly through the client
    or by using the methods on related objects.
    """

    #: The supported protocols
    PROTOCOLS = ['http', 'https', 'pbc']

    def __init__(self, protocol='http', transport_options={},
                 nodes=None, **unused_args):
        """
        Construct a new ``RiakClient`` object.

        :param protocol: the preferred protocol, defaults to 'http'
        :type protocol: string
        :param nodes: a list of node configurations,
           where each configuration is a dict containing the keys
           'host', 'http_port', and 'pb_port'
        :type nodes: list
        :param transport_options: Optional key-value args to pass to
                                  the transport constructor
        :type transport_options: dict
        """
        unused_args = unused_args.copy()

        if 'port' in unused_args:
            deprecated("port option is deprecated, use http_port or pb_port,"
                       " or the nodes option. Your given port of %r will be "
                       "used as the %s port unless already set" %
                       (unused_args['port'], protocol))
            unused_args['already_warned_port'] = True
            if (protocol in ['http', 'https'] and
                    'http_port' not in unused_args):
                unused_args['http_port'] = unused_args['port']
            elif protocol == 'pbc' and 'pb_port' not in unused_args:
                unused_args['pb_port'] = unused_args['port']

        if 'transport_class' in unused_args:
            deprecated(
                "transport_class is deprecated, use the protocol option")

        if nodes is None:
            self.nodes = [self._create_node(unused_args), ]
        else:
            self.nodes = [self._create_node(n) for n in nodes]

        self.protocol = protocol or 'http'
        self.resolver = default_resolver
        self._http_pool = RiakHttpPool(self, **transport_options)
        self._pb_pool = RiakPbcPool(self, **transport_options)

        self._encoders = {'application/json': default_encoder,
                          'text/json': default_encoder,
                          'text/plain': str}
        self._decoders = {'application/json': json.loads,
                          'text/json': json.loads,
                          'text/plain': str}
        self._buckets = WeakValueDictionary()

    def _get_protocol(self):
        return self._protocol

    def _set_protocol(self, value):
        if value not in self.PROTOCOLS:
            raise ValueError("protocol option is invalid, must be one of %s" %
                             repr(self.PROTOCOLS))
        self._protocol = value

    protocol = property(_get_protocol, _set_protocol,
                        doc=
                        """
                        Which protocol to prefer, one of
                        :attr:`PROTOCOLS
                        <riak.client.RiakClient.PROTOCOLS>`. Please
                        note that when one protocol is selected, the
                        other protocols MAY NOT attempt to connect.
                        Changing to another protocol will cause a
                        connection on the next request.

                        Some requests are only valid over ``'http'``
                        or ``'https'``, and will always be sent via
                        those transports, regardless of which protocol
                        is preferred.
                         """)

    def get_transport(self):
        """
        Get the transport instance the client is using for it's
        connection.

        .. deprecated:: 2.0.0
           There is no equivalent to this method, it will return ``None``.
        """
        deprecated("get_transport is deprecated, use client, " +
                   "bucket, or object methods instead")
        return None

    def get_client_id(self):
        """
        Get the client identifier.

        .. deprecated:: 2.0.0
           Use the :attr:`client_id` attribute instead.

        :rtype: string
        """
        deprecated(
            "``get_client_id`` is deprecated, use the ``client_id`` property")
        return self.client_id

    def set_client_id(self, client_id):
        """
        Set the client identifier.

        .. deprecated:: 2.0.0
           Use the :attr:`client_id` attribute instead.

        :param client_id: The new client_id.
        :type client_id: string
        """
        deprecated(
            "``set_client_id`` is deprecated, use the ``client_id`` property")
        self.client_id = client_id
        return self

    def _get_client_id(self):
        with self._transport() as transport:
            return transport.client_id

    def _set_client_id(self, client_id):
        for http in self._http_pool:
            http.client_id = client_id
        for pb in self._pb_pool:
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
            argument and returns a string
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
        :param decoder: a decoding function, takes a string and
            returns a Python type
        :type decoder: function
        """
        self._decoders[content_type] = decoder

    def bucket(self, name):
        """
        Get the bucket by the specified name. Since buckets always exist,
        this will always return a :class:`RiakBucket <riak.bucket.RiakBucket>`.

        :rtype: :class:`RiakBucket <riak.bucket.RiakBucket>`
        """
        if name in self._buckets:
            return self._buckets[name]
        else:
            bucket = RiakBucket(self, name)
            self._buckets[name] = bucket
            return bucket

    @lazy_property
    def solr(self):
        """
        Returns a RiakSearch object which can access search indexes.

        .. deprecated:: 2.0.0
           Use the ``fulltext_*`` methods instead.
        """
        deprecated("``solr`` is deprecated, use ``fulltext_search``,"
                   " ``fulltext_add`` and ``fulltext_delete`` directly")
        return RiakSearch(self)

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
