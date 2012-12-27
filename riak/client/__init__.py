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
    import json
except ImportError:
    import simplejson as json

from contextlib import contextmanager
from weakref import WeakValueDictionary
from riak.client.operations import RiakClientOperations
from riak.client.transport import RiakClientTransport
from riak.node import RiakNode
from riak.bucket import RiakBucket
from riak.mapreduce import RiakMapReduce
from riak.mapreduce import RiakMapReduceChain
from riak.search import RiakSearch
from riak.transports.http import RiakHttpPool
from riak.transports.pbc import RiakPbcPool
from riak.util import deprecated
from riak.util import deprecateQuorumAccessors
from riak.util import lazy_property


@deprecateQuorumAccessors
class RiakClient(RiakMapReduceChain, RiakClientOperations,
                 RiakClientTransport):
    """
    The ``RiakClient`` object holds information necessary to connect
    to Riak. Requests can be made to Riak directly through the client
    or by using the methods on related objects.
    """

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
                                  the transport constuctor
        :type transport_options: dict
        """
        if 'port' in unused_args:
            deprecated("port option is deprecated, use http_port or pb_port,"
                      + " or the nodes option")

        if 'transport_class' in unused_args:
            deprecated(
                "transport_class is deprecated, use the protocol option")

        if nodes is None:
            self.nodes = [self._create_node(unused_args), ]
        else:
            self.nodes = [self._create_node(n) for n in nodes]

        self.protocol = protocol or 'http'

        self._http_pool = RiakHttpPool(self, **transport_options)
        self._pb_pool = RiakPbcPool(self, **transport_options)

        self._encoders = {'application/json': json.dumps,
                          'text/json': json.dumps}
        self._decoders = {'application/json': json.loads,
                          'text/json': json.loads}
        self._buckets = WeakValueDictionary()

    @property
    def protocol(self):
        return self._protocol

    @property.setter
    def protocol(self, value):
        if value not in self.PROTOCOLS:
            raise ValueError("protocol option is invalid, must be one of %s" %
                             repr(self.PROTOCOLS))
        self._protocol = value

    def get_transport(self):
        """
        Get the transport instance the client is using for it's connection.
        """
        deprecated("get_transport is deprecated, use client, " +
                   "bucket, or object methods instead")
        return None

    def get_client_id(self):
        """
        Get the ``client_id`` for this ``RiakClient`` instance.
        DEPRECATED

        :rtype: string
        """
        deprecated(
            "``get_client_id`` is deprecated, use the ``client_id`` property")
        return self.client_id

    def set_client_id(self, client_id):
        """
        Set the client_id for this ``RiakClient`` instance.

        :param client_id: The new client_id.
        :type client_id: string
        """
        deprecated(
            "``set_client_id`` is deprecated, use the ``client_id`` property")
        self.client_id = client_id
        return self

    @property
    def client_id(self):
        """
        The client ID for this client instance

        :rtype: string
        """
        with self.transport() as transport:
            return transport.get_client_id()

    @client_id.setter
    def client_id(self, client_id):
        for http in self._http_pool:
            http.client_id = client_id
        for pb in self._pb_pool:
            pb.client_id = client_id

    def get_encoder(self, content_type):
        """
        Get the encoding function for the provided content type.
        """
        return self._encoders.get(content_type)

    def set_encoder(self, content_type, encoder):
        """
        Set the encoding function for the provided content type.

        :param encoder:
        :type encoder: function
        """
        self._encoders[content_type] = encoder

    def get_decoder(self, content_type):
        """
        Get the decoding function for the provided content type.
        """
        return self._decoders.get(content_type)

    def set_decoder(self, content_type, decoder):
        """
        Set the decoding function for the provided content type.

        :param decoder:
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
        """
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

    def _choose_node(self, nodes=self.nodes):
        """
        Chooses a random node from the list of nodes in the client,
        taking into account each node's recent error rate.
        :rtype RiakNode
        """
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
