"""
Copyright 2012 Basho Technologies, Inc.
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

import errno
import socket
from riak.transports.pool import Pool
from riak.transports.pbc.transport import RiakPbcTransport


class RiakPbcPool(Pool):
    """
    A resource pool of PBC transports.
    """
    def __init__(self, client, **options):
        super(RiakPbcPool, self).__init__()
        self._client = client
        self._options = options

    def create_resource(self):
        node = self._client._choose_node()
        return RiakPbcTransport(node=node,
                                client=self._client,
                                **self._options)

    def destroy_resource(self, pbc):
        pbc.close()

# These are a specific set of socket errors
# that could be raised on send/recv that indicate
# that the socket is closed or reset, and is not
# usable. On seeing any of these errors, the socket
# should be closed, and the connection re-established.
CONN_CLOSED_ERRORS = (
    errno.EHOSTUNREACH,
    errno.ECONNRESET,
    errno.ECONNREFUSED,
    errno.ECONNABORTED,
    errno.ETIMEDOUT,
    errno.EBADF,
    errno.EPIPE
)


def is_retryable(err):
    """
    Determines if the given exception is something that is
    network/socket-related and should thus cause the PBC connection to
    close and the operation retried on another node.

    :rtype: boolean
    """
    if isinstance(err, socket.error):
        code = err.args[0]
        return code in CONN_CLOSED_ERRORS
    else:
        return False
