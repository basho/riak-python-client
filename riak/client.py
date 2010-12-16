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
# Use json as first choice, simplejson as second choice.
try:
    import json
except ImportError:
    import simplejson as json

from riak.transports import RiakHttpTransport
from riak.bucket import RiakBucket
from riak.mapreduce import RiakMapReduce, RiakLink

class RiakClient(object):
    """
    The ``RiakClient`` object holds information necessary to connect to
    Riak. The Riak API uses HTTP, so there is no persistent
    connection, and the ``RiakClient`` object is extremely lightweight.
    """
    def __init__(self, host='127.0.0.1', port=8098, prefix='riak',
                 mapred_prefix='mapred', transport_class=None,
                 client_id=None):
        """
        Construct a new ``RiakClient`` object.

        :param host: Hostname or IP address
        :type host: string
        :param port: Port number
        :type port: integer
        :param prefix: Interface prefix
        :type prefix: string
        :param mapred_prefix: MapReduce prefix
        :type mapred_prefix: string
        :param transport_class: transport class to use
        :type transport_class: :class:`RiakTransport`
        """
        if not transport_class:
            self._transport = RiakHttpTransport(host,
                                                port,
                                                prefix,
                                                mapred_prefix,
                                                client_id)
        else:
            self._transport = transport_class(host, port, client_id=client_id)
        self._r = "default"
        self._w = "default"
        self._dw = "default"
        self._rw = "default"
        self._encoders = {'application/json':json.dumps,
                          'text/json':json.dumps}
        self._decoders = {'application/json':json.loads,
                          'text/json':json.loads}

    def get_transport(self):
        """
        Get the transport instance the client is using for it's connection.
        """
        return self._transport;

    def get_r(self):
        """
        Get the R-value setting for this ``RiakClient``. (default "quorum")

        :rtype: integer
        """
        return self._r

    def set_r(self, r):
        """
        Set the R-value for this ``RiakClient``. This value will be used
        for any calls to :func:`RiakBucket.get <riak.bucket.RiakBucket.get>`
        or :func:`RiakBucket.get_binary <riak.bucket.RiakBucket.get_binary>`
        where 1) no R-value is specified in the method call and 2) no R-value has
        been set in the :class:`RiakBucket <riak.bucket.RiakBucket>`.

        :param r: The R value.
        :type r: integer
        :rtype: self
        """
        self._r = r
        return self

    def get_w(self):
        """
        Get the W-value setting for this ``RiakClient``. (default "quorum")

        :rtype: integer
        """
        return self._w

    def set_w(self, w):
        """
        Set the W-value for this ``RiakClient`` instance. See :func:`set_r` for a
        description of how these values are used.

        :param w: The W value.
        :type w: integer
        :rtype: self
        """
        self._w = w
        return self

    def get_dw(self):
        """
        Get the DW-value for this ``RiakClient`` instance. (default "quorum")

        :rtype: integer
        """
        return self._dw

    def set_dw(self, dw):
        """
        Set the DW-value for this ``RiakClient`` instance. See :func:`set_r` for a
        description of how these values are used.

        :param dw: The DW value.
        :type dw: integer
        :rtype: self
        """
        self._dw = dw
        return self

    def get_rw(self):
        """
        Get the RW-value for this ``RiakClient`` instance. (default "quorum")

        :rtype: integer
        """
        return self._rw

    def set_rw(self, rw):
        """
        Set the RW-value for this ``RiakClient`` instance. See :func:`set_r` for a
        description of how these values are used.

        :param rw: The RW value.
        :type rw: integer
        :rtype: self
        """
        self._rw = rw
        return self

    def get_client_id(self):
        """
        Get the ``client_id`` for this ``RiakClient`` instance.

        :rtype: string
        """
        return self._transport.get_client_id()

    def set_client_id(self, client_id):
        """
        Set the client_id for this ``RiakClient`` instance.

        .. warning::

           You should not call this method unless you know what you are doing.

        :param client_id: The new client_id.
        :type client_id: string
        :rtype: self
        """
        self._transport.set_client_id(client_id)
        return self

    def get_encoder(self, content_type):
        """
        Get the encoding function for the provided content type.
        """
        if content_type in self._encoders:
            return self._encoders[content_type]

    def set_encoder(self, content_type, encoder):
        """
        Set the encoding function for the provided content type.

        :param encoder:
        :type encoder: function
        """
        self._encoders[content_type] = encoder
        return self

    def get_decoder(self, content_type):
        """
        Get the decoding function for the provided content type.
        """
        if content_type in self._decoders:
            return self._decoders[content_type]

    def set_decoder(self, content_type, decoder):
        """
        Set the decoding function for the provided content type.

        :param decoder:
        :type decoder: function
        """
        self._decoders[content_type] = decoder
        return self

    def bucket(self, name):
        """
        Get the bucket by the specified name. Since buckets always exist,
        this will always return a :class:`RiakBucket <riak.bucket.RiakBucket>`.

        :rtype: :class:`RiakBucket <riak.bucket.RiakBucket>`
        """
        return RiakBucket(self, name)

    def is_alive(self):
        """
        Check if the Riak server for this ``RiakClient`` instance is alive.

        :rtype: boolean
        """
        return self._transport.ping()

    def add(self, *args):
        """
        Start assembling a Map/Reduce operation. A shortcut for :func:`RiakMapReduce.add`.

        :rtype: :class:`RiakMapReduce`
        """
        mr = RiakMapReduce(self)
        return apply(mr.add, args)

    def search(self, *args):
        """
        Start assembling a Map/Reduce operation based on search
        results. This command will return an error unless executed
        against a Riak Search cluster. A shortcut for :func:`RiakMapReduce.search`.

        :rtype: :class:`RiakMapReduce`
        """
        mr = RiakMapReduce(self)
        return apply(mr.search, args)

    def link(self, *args):
        """
        Start assembling a Map/Reduce operation. A shortcut for :func:`RiakMapReduce.link`.

        :rtype: :class:`RiakMapReduce`
        """
        mr = RiakMapReduce(self)
        return apply(mr.link, args)

    def map(self, *args):
        """
        Start assembling a Map/Reduce operation. A shortcut for :func:`RiakMapReduce.map`.

        :rtype: :class:`RiakMapReduce`
        """
        mr = RiakMapReduce(self)
        return apply(mr.map, args)

    def reduce(self, *args):
        """
        Start assembling a Map/Reduce operation. A shortcut for :func:`RiakMapReduce.reduce`.

        :rtype: :class:`RiakMapReduce`
        """
        mr = RiakMapReduce(self)
        return apply(mr.reduce, args)
