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
    The RiakClient object holds information necessary to connect to
    Riak. The Riak API uses HTTP, so there is no persistent
    connection, and the RiakClient object is extremely lightweight.
    """
    def __init__(self, host='127.0.0.1', port=8098, prefix='riak',
                 mapred_prefix='mapred', transport_class=None,
                 client_id=None):
        """
        Construct a new RiakClient object.
        @param string host - Hostname or IP address (default '127.0.0.1')
        @param int port - Port number (default 8098)
        @param string prefix - Interface prefix (default 'riak')
        @param string mapred_prefix - MapReduce prefix (default 'mapred')
        @param RiakTransport transport_class - transport class to use
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
        Get a transport object
        """
        return self._transport;

    def get_r(self):
        """
        Get the R-value setting for this RiakClient. (default "quorum")
        @return integer
        """
        return self._r

    def set_r(self, r):
        """
        Set the R-value for this RiakClient. This value will be used
        for any calls to get(...) or get_binary(...) where where 1) no
        R-value is specified in the method call and 2) no R-value has
        been set in the RiakBucket.
        @param integer r - The R value.
        @return self
        """
        self._r = r
        return self

    def get_w(self):
        """
        Get the W-value setting for this RiakClient. (default "quorum")
        @return integer
        """
        return self._w

    def set_w(self, w):
        """
        Set the W-value for this RiakClient. See set_r(...) for a
        description of how these values are used.
        @param integer w - The W value.
        @return self
        """
        self._w = w
        return self

    def get_dw(self):
        """
        Get the DW-value for this ClientOBject. (default "quorum")
        @return integer
        """
        return self._dw

    def set_dw(self, dw):
        """
        Set the DW-value for this RiakClient. See set_r(...) for a
        description of how these values are used.
        @param integer dw - The DW value.
        @return self
        """
        self._dw = dw
        return self

    def get_rw(self):
        """
        Get the RW-value for this ClientObject. (default "quorum")
        @return integer
        """
        return self._rw

    def set_rw(self, rw):
        """
        Set the RW-value for this RiakClient. See set_r(...) for a
        description of how these values are used.
        @param integer rw - The RW value.
        @return self
        """
        self._rw = rw
        return self

    def get_client_id(self):
        """
        Get the client_id for this RiakClient.
        @return string
        """
        return self._transport.get_client_id()

    def set_client_id(self, client_id):
        """
        Set the client_id for this RiakClient. Should not be called
        unless you know what you are doing.
        @param string client_id - The new client_id.
        @return self
        """
        self._transport.set_client_id(client_id)
        return self

    def get_encoder(self, content_type):
        """
        Get the encoding function for this content type
        """
        if content_type in self._encoders:
            return self._encoders[content_type]

    def set_encoder(self, content_type, encoder):
        """
        Set the encoding function for this content type
        @param function encoder
        """
        self._encoders[content_type] = encoder
        return self

    def get_decoder(self, content_type):
        """
        Get the decoding function for this content type
        """
        if content_type in self._decoders:
            return self._decoders[content_type]

    def set_decoder(self, content_type, decoder):
        """
        Set the decoding function for this content type
        @param function decoder
        """
        self._decoders[content_type] = decoder
        return self

    def bucket(self, name):
        """
        Get the bucket by the specified name. Since buckets always exist,
        this will always return a RiakBucket.
        @return RiakBucket
        """
        return RiakBucket(self, name)

    def is_alive(self):
        """
        Check if the Riak server for this RiakClient is alive.
        @return boolean
        """
        return self._transport.ping()

    def add(self, *args):
        """
        Start assembling a Map/Reduce operation.
        @see RiakMapReduce.add()
        @return RiakMapReduce
        """
        mr = RiakMapReduce(self)
        return apply(mr.add, args)

    def link(self, args):
        """
        Start assembling a Map/Reduce operation.
        @see RiakMapReduce.link()
        """
        mr = RiakMapReduce(self)
        return apply(mr.link, args)

    def map(self, *args):
        """
        Start assembling a Map/Reduce operation.
        @see RiakMapReduce.map()
        """
        mr = RiakMapReduce(self)
        return apply(mr.map, args)

    def reduce(self, *args):
        """
        Start assembling a Map/Reduce operation.
        @see RiakMapReduce.reduce()
        """
        mr = RiakMapReduce(self)
        return apply(mr.reduce, args)
