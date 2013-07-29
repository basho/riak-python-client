"""
Copyright 2012 Basho Technologies, Inc.

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

from distutils.version import LooseVersion
from riak.util import lazy_property


versions = {
    1: LooseVersion("1.0.0"),
    1.1: LooseVersion("1.1.0"),
    1.2: LooseVersion("1.2.0"),
    1.4: LooseVersion("1.4.0")
}


class FeatureDetection(object):
    """
    Implements boolean methods that can be checked for the presence of
    specific server-side features. Subclasses must implement the
    :meth:`_server_version` method to use this functionality, which
    should return the server's version as a string.

    :class:`FeatureDetection` is a parent class of
    :class:`RiakTransport <riak.transports.transport.RiakTransport>`.
    """

    def _server_version(self):
        """
        Gets the server version from the server. To be implemented by
        the individual transport class.

        :rtype: string
        """
        raise NotImplementedError

    def phaseless_mapred(self):
        """
        Whether MapReduce requests can be submitted without phases.

        :rtype: bool
        """
        return self.server_version >= versions[1.1]

    def pb_indexes(self):
        """
        Whether secondary index queries are supported over Protocol
        Buffers

        :rtype: bool
        """
        return self.server_version >= versions[1.2]

    def pb_search(self):
        """
        Whether search queries are supported over Protocol Buffers

        :rtype: bool
        """
        return self.server_version >= versions[1.2]

    def pb_conditionals(self):
        """
        Whether conditional fetch/store semantics are supported over
        Protocol Buffers

        :rtype: bool
        """
        return self.server_version >= versions[1]

    def quorum_controls(self):
        """
        Whether additional quorums and FSM controls are available,
        e.g. primary quorums, basic_quorum, notfound_ok

        :rtype: bool
        """
        return self.server_version >= versions[1]

    def tombstone_vclocks(self):
        """
        Whether 'not found' responses might include vclocks

        :rtype: bool
        """
        return self.server_version >= versions[1]

    def pb_head(self):
        """
        Whether partial-fetches (vclock and metadata only) are
        supported over Protocol Buffers

        :rtype: bool
        """
        return self.server_version >= versions[1]

    def pb_clear_bucket_props(self):
        """
        Whether bucket properties can be cleared over Protocol
        Buffers.

        :rtype: bool
        """
        return self.server_version >= versions[1.4]

    def pb_all_bucket_props(self):
        """
        Whether all normal bucket properties are supported over
        Protocol Buffers.

        :rtype: bool
        """
        return self.server_version >= versions[1.4]

    def counters(self):
        """
        Whether CRDT counters are supported.

        :rtype: bool
        """
        return self.server_version >= versions[1.4]

    def bucket_stream(self):
        """
        Whether streaming bucket lists are supported.

        :rtype: bool
        """
        return self.server_version >= versions[1.4]

    def client_timeouts(self):
        """
        Whether client-supplied timeouts are supported.

        :rtype: bool
        """
        return self.server_version >= versions[1.4]

    def stream_indexes(self):
        """
        Whether secondary indexes support streaming responses.

        :rtype: bool
        """
        return self.server_version >= versions[1.4]

    @lazy_property
    def server_version(self):
        return LooseVersion(self._server_version())
