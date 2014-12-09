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

import socket
import struct
import riak_pb
from riak.security import SecurityError
from riak import RiakError
from riak_pb.messages import (
    MESSAGE_CLASSES,
    MSG_CODE_ERROR_RESP,
    MSG_CODE_START_TLS,
    MSG_CODE_AUTH_REQ,
    MSG_CODE_AUTH_RESP
)
from riak.util import bytes_to_str, str_to_bytes
from six import PY2
if PY2:
    from OpenSSL.SSL import Connection
    from riak.transports.security import configure_pyopenssl_context
else:
    import ssl
    from riak.transports.security import configure_ssl_context


class RiakPbcConnection(object):
    """
    Connection-related methods for RiakPbcTransport.
    """

    def _encode_msg(self, msg_code, msg=None):
        if msg is None:
            return struct.pack("!iB", 1, msg_code)
        msgstr = msg.SerializeToString()
        slen = len(msgstr)
        hdr = struct.pack("!iB", 1 + slen, msg_code)
        return hdr + msgstr

    def _request(self, msg_code, msg=None, expect=None):
        self._send_msg(msg_code, msg)
        return self._recv_msg(expect)

    def _non_connect_request(self, msg_code, msg=None, expect=None):
        """
        Similar to self._request, but doesn't try to initiate a connection,
        thus preventing an infinite loop.
        """
        self._non_connect_send_msg(msg_code, msg)
        return self._recv_msg(expect)

    def _non_connect_send_msg(self, msg_code, msg):
        """
        Similar to self._send, but doesn't try to initiate a connection,
        thus preventing an infinite loop.
        """
        self._socket.sendall(self._encode_msg(msg_code, msg))

    def _send_msg(self, msg_code, msg):
        self._connect()
        self._non_connect_send_msg(msg_code, msg)

    def _init_security(self):
        """
        Initialize a secure connection to the server.
        """
        if not self._starttls():
            raise SecurityError("Could not start TLS connection")
        # _ssh_handshake() will throw an exception upon failure
        self._ssl_handshake()
        if not self._auth():
            raise SecurityError("Could not authorize connection")

    def _starttls(self):
        """
        Exchange a STARTTLS message with Riak to initiate secure communications
        return True is Riak responds with a STARTTLS response, False otherwise
        """
        msg_code, _ = self._non_connect_request(MSG_CODE_START_TLS)
        if msg_code == MSG_CODE_START_TLS:
            return True
        else:
            return False

    def _auth(self):
        """
        Perform an authorization request against Riak
        returns True upon success, False otherwise
        Note: Riak will sleep for a short period of time upon a failed
              auth request/response to prevent denial of service attacks
        """
        req = riak_pb.RpbAuthReq()
        req.user = str_to_bytes(self._client._credentials.username)
        req.password = str_to_bytes(self._client._credentials.password)
        msg_code, _ = self._non_connect_request(MSG_CODE_AUTH_REQ, req,
                                                MSG_CODE_AUTH_RESP)
        if msg_code == MSG_CODE_AUTH_RESP:
            return True
        else:
            return False

    if PY2:
        def _ssl_handshake(self):
            """
            Perform an SSL handshake w/ the server.
            Precondition: a successful STARTTLS exchange has
                         taken place with Riak
            returns True upon success, otherwise an exception is raised
            """
            if self._client._credentials:
                try:
                    ssl_ctx = configure_pyopenssl_context(self.
                                                          _client._credentials)
                    # attempt to upgrade the socket to SSL
                    ssl_socket = Connection(ssl_ctx, self._socket)
                    ssl_socket.set_connect_state()
                    ssl_socket.do_handshake()
                    # ssl handshake successful
                    self._socket = ssl_socket

                    self._client._credentials._check_revoked_cert(ssl_socket)
                    return True
                except Exception as e:
                    # fail if *any* exceptions are thrown during SSL handshake
                    raise SecurityError(e.message)
    else:
        def _ssl_handshake(self):
            """
            Perform an SSL handshake w/ the server.
            Precondition: a successful STARTTLS exchange has
                         taken place with Riak
            returns True upon success, otherwise an exception is raised
            """
            credentials = self._client._credentials
            if credentials:
                try:
                    ssl_ctx = configure_ssl_context(credentials)
                    host = "riak@" + self._address[0]
                    ssl_socket = ssl.SSLSocket(sock=self._socket,
                                               keyfile=credentials.pkey_file,
                                               certfile=credentials.cert_file,
                                               cert_reqs=ssl.CERT_REQUIRED,
                                               ca_certs=credentials.
                                               cacert_file,
                                               ciphers=credentials.ciphers,
                                               server_hostname=host)
                    ssl_socket.context = ssl_ctx
                    # ssl handshake successful
                    ssl_socket.do_handshake()
                    self._socket = ssl_socket

                    return True
                except ssl.SSLError as e:
                    raise SecurityError(e.library + ": " + e.reason)
                except Exception as e:
                    # fail if *any* exceptions are thrown during SSL handshake
                    raise SecurityError(e)

    def _recv_msg(self, expect=None):
        self._recv_pkt()
        msg_code, = struct.unpack("B", self._inbuf[:1])
        if msg_code is MSG_CODE_ERROR_RESP:
            err = self._parse_msg(msg_code, self._inbuf[1:])
            raise RiakError(bytes_to_str(err.errmsg))
        elif msg_code in MESSAGE_CLASSES:
            msg = self._parse_msg(msg_code, self._inbuf[1:])
        else:
            raise Exception("unknown msg code %s" % msg_code)

        if expect and msg_code != expect:
            raise RiakError("unexpected protocol buffer message code: %d, %r"
                            % (msg_code, msg))
        return msg_code, msg

    def _recv_pkt(self):
        nmsglen = self._socket.recv(4)
        while len(nmsglen) < 4:
            x = self._socket.recv(4 - len(nmsglen))
            if not x:
                break
            nmsglen += x
        if len(nmsglen) != 4:
            raise RiakError(
                "Socket returned short packet length %d - expected 4"
                % len(nmsglen))
        msglen, = struct.unpack('!i', nmsglen)
        self._inbuf_len = msglen
        if PY2:
            self._inbuf = ''
        else:
            self._inbuf = bytes()
        while len(self._inbuf) < msglen:
            want_len = min(8192, msglen - len(self._inbuf))
            recv_buf = self._socket.recv(want_len)
            if not recv_buf:
                break
            self._inbuf += recv_buf
        if len(self._inbuf) != self._inbuf_len:
            raise RiakError("Socket returned short packet %d - expected %d"
                            % (len(self._inbuf), self._inbuf_len))

    def _connect(self):
        if not self._socket:
            if self._timeout:
                self._socket = socket.create_connection(self._address,
                                                        self._timeout)
            else:
                self._socket = socket.create_connection(self._address)
            if self._client._credentials:
                self._init_security()

    def close(self):
        """
        Closes the underlying socket of the PB connection.
        """
        if self._socket:
            self._socket.close()
            del self._socket

    def _parse_msg(self, code, packet):
        try:
            pbclass = MESSAGE_CLASSES[code]
        except KeyError:
            pbclass = None

        if pbclass is None:
            return None

        pbo = pbclass()
        pbo.ParseFromString(packet)
        return pbo

    # These are set in the RiakPbcTransport initializer
    _address = None
    _timeout = None
