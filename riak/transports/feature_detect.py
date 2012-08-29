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

from distutils.version import StrictVersion
from riak.util import lazy_property


versions = {
    1: StrictVersion("1.0.0"),
    1.1: StrictVersion("1.1.0"),
    1.2: StrictVersion("1.2.0")
    }


class FeatureDetection(object):
    def _server_version(self):
        """
        Gets the server version from the server. To be implemented by
        the individual transport class.
        :rtype string
        """
        raise NotImplementedError

    def phaseless_mapred(self):
        """
        Whether MapReduce requests can be submitted without phases.
        :rtype bool
        """
        return self.server_version >= versions[1.1]

    def pb_indexes(self):
        """
        Whether secondary index queries are supported over Protocol
        Buffers

        :rtype bool
        """
        return self.server_version >= versions[1.2]

    def pb_search(self):
        """
        Whether search queries are supported over Protocol Buffers
        :rtype bool
        """
        return self.server_version >= versions[1.2]

    def pb_conditionals(self):
        """
        Whether conditional fetch/store semantics are supported over
        Protocol Buffers
        :rtype bool
        """
        return self.server_version >= versions[1]

    def quorum_controls(self):
        """
        Whether additional quorums and FSM controls are available,
        e.g. primary quorums, basic_quorum, notfound_ok
        :rtype bool
        """
        return self.server_version >= versions[1]

    def tombstone_vclocks(self):
        """
        Whether 'not found' responses might include vclocks
        :rtype bool
        """
        return self.server_version >= versions[1]

    def pb_head(self):
        """
        Whether partial-fetches (vclock and metadata only) are
        supported over Protocol Buffers
        :rtype bool
        """
        return self.server_version >= versions[1]

    @lazy_property
    def server_version(self):
        return StrictVersion(self._server_version())
