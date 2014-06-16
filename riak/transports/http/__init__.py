"""
Copyright 2014 Basho Technologies, Inc.

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

import OpenSSL.SSL
import httplib
import socket
import select

from riak.security import SecurityError, check_revoked_cert
from riak.transports.security import RiakWrappedSocket, configure_context
from riak.transports.pool import Pool
from riak.transports.http.transport import RiakHttpTransport


class NoNagleHTTPConnection(httplib.HTTPConnection):
    """
    Setup a connection class which does not use Nagle - deal with
    latency on PUT requests lower than MTU
    """
    def connect(self):
        """
        Set TCP_NODELAY on socket
        """
        httplib.HTTPConnection.connect(self)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


# Inspired by
# http://code.activestate.com/recipes/577548-https-httplib-client-connection-with-certificate-v/
class RiakHTTPSConnection(httplib.HTTPSConnection):
    def __init__(self,
                 host,
                 port,
                 credentials,
                 key_file=None,
                 cert_file=None,
                 ciphers=None,
                 timeout=None):
        """
        Class to make a HTTPS connection,
        with support for full client-based SSL Authentication

        :param host: Riak host name
        :type host: str
        :param port: Riak host port number
        :type port: int
        :param credentials: Security Credential settings
        :type  credentials: SecurityCreds
        :param key_file: PEM formatted file that contains your private key
        :type key_file: str
        :param cert_file: PEM formatted certificate chain file
        :type key_file: str
        :param ciphers: List of supported SSL ciphers
        :type ciphers: str
        :param timeout: Number of seconds before timing out
        :type timeout: int
        """
        httplib.HTTPSConnection.__init__(self,
                                         host,
                                         port,
                                         key_file=key_file,
                                         cert_file=cert_file)
        self.key_file = key_file
        self.cert_file = cert_file
        self.credentials = credentials
        self.timeout = timeout

    def connect(self):
        """
        Connect to a host on a given (SSL) port using PyOpenSSL.
        """
        sock = socket.create_connection((self.host, self.port), self.timeout)
        ssl_ctx = OpenSSL.SSL.Context(self.credentials.ssl_version)
        configure_context(ssl_ctx, self.credentials)

        # attempt to upgrade the socket to SSL
        cxn = OpenSSL.SSL.Connection(ssl_ctx, sock)
        cxn.set_connect_state()
        while True:
            try:
                cxn.do_handshake()
            except OpenSSL.SSL.WantReadError:
                select.select([sock], [], [])
                continue
            except OpenSSL.SSL.Error as e:
                raise SecurityError('bad handshake - ' + str(e))
            break

        self.sock = RiakWrappedSocket(cxn, sock)
        if self.credentials.crl_file:
            check_revoked_cert(self.sock, self.credentials.crl_file)


class RiakHttpPool(Pool):
    """
    A pool of HTTP(S) transport connections.
    """
    def __init__(self, client, **options):
        self.client = client
        self.options = options
        self.connection_class = NoNagleHTTPConnection
        if self.client._credentials:
            self.connection_class = RiakHTTPSConnection

        super(RiakHttpPool, self).__init__()

    def create_resource(self):
        node = self.client._choose_node()
        return RiakHttpTransport(node=node,
                                 client=self.client,
                                 connection_class=self.connection_class,
                                 **self.options)

    def destroy_resource(self, transport):
        transport.close()


CONN_CLOSED_ERRORS = (
    httplib.NotConnected,
    httplib.IncompleteRead,
    httplib.ImproperConnectionState,
    httplib.BadStatusLine
)


def is_retryable(err):
    """
    Determines if the given exception is something that is
    network/socket-related and should thus cause the HTTP connection
    to close and the operation retried on another node.

    :rtype: boolean
    """
    for errtype in CONN_CLOSED_ERRORS:
        if isinstance(err, errtype):
            return True
    return False
