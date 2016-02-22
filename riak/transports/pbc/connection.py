import logging
import socket
import struct
import riak.pb.riak_pb2
import riak.pb.messages
import erlastic

from riak.security import SecurityError, USE_STDLIB_SSL
from riak import RiakError
from riak.util import bytes_to_str, str_to_bytes

from six import PY2
if not USE_STDLIB_SSL:
    from OpenSSL.SSL import Connection
    from riak.transports.security import configure_pyopenssl_context
else:
    import ssl
    from riak.transports.security import configure_ssl_context


class RiakPbcConnection(object):
    """
    Connection-related methods for RiakPbcTransport.
    """

    def __init__(self):
        self._ttb_enabled = False

    def _encode_msg(self, msg_code, msg=None, is_ttb=False):
        if msg is None:
            return struct.pack("!iB", 1, msg_code)

        if is_ttb:
            data = msg
        else:
            data = msg.SerializeToString()

        datalen = len(data)
        hdr = struct.pack("!iB", 1 + datalen, msg_code)
        return hdr + data

    def _request(self, msg_code, msg=None, expect=None, is_ttb=False):
        self._send_msg(msg_code, msg, is_ttb)
        return self._recv_msg(expect, is_ttb)

    def _non_connect_request(self, msg_code, msg=None, expect=None):
        """
        Similar to self._request, but doesn't try to initiate a connection,
        thus preventing an infinite loop.
        """
        self._non_connect_send_msg(msg_code, msg)
        return self._recv_msg(expect)

    def _non_connect_send_msg(self, msg_code, msg, is_ttb=False):
        """
        Similar to self._send, but doesn't try to initiate a connection,
        thus preventing an infinite loop.
        """
        self._socket.sendall(self._encode_msg(msg_code, msg, is_ttb))

    def _send_msg(self, msg_code, msg, is_ttb=False):
        self._connect()
        if is_ttb and not self._enable_ttb():
            raise RiakError('could not switch to TTB encoding!')
        self._non_connect_send_msg(msg_code, msg, is_ttb)

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
        msg_code, _ = self._non_connect_request(
            riak.pb.messages.MSG_CODE_START_TLS)
        if msg_code == riak.pb.messages.MSG_CODE_START_TLS:
            return True
        else:
            return False

    def _enable_ttb(self):
        if self._ttb_enabled:
            return True
        else:
            logging.debug("pbc/connection enabling TTB")
            msg_code, _ = self._non_connect_request(
                riak.pb.messages.MSG_CODE_TOGGLE_ENCODING_REQ)
            if msg_code == riak.pb.messages.MSG_CODE_TOGGLE_ENCODING_RESP:
                self._ttb_enabled = True
                logging.debug("pbc/connection TTB IS ENABLED")
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
        req = riak.pb.riak_pb2.RpbAuthReq()
        req.user = str_to_bytes(self._client._credentials.username)
        password = self._client._credentials.password
        if not password:
            password = ''
        req.password = str_to_bytes(password)
        msg_code, _ = self._non_connect_request(
            riak.pb.messages.MSG_CODE_AUTH_REQ,
            req,
            riak.pb.messages.MSG_CODE_AUTH_RESP)
        if msg_code == riak.pb.messages.MSG_CODE_AUTH_RESP:
            return True
        else:
            return False

    if not USE_STDLIB_SSL:
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
                    raise SecurityError(e)
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
                    host = self._address[0]
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
                    raise SecurityError(e)
                except Exception as e:
                    # fail if *any* exceptions are thrown during SSL handshake
                    raise SecurityError(e)

    def _recv_msg(self, expect=None, is_ttb=False):
        self._recv_pkt()
        msg_code, = struct.unpack("B", self._inbuf[:1])
        if msg_code is riak.pb.messages.MSG_CODE_ERROR_RESP:
            err = self._parse_msg(msg_code, self._inbuf[1:], is_ttb)
            if err is None:
                raise RiakError('no error provided!')
            else:
                raise RiakError(bytes_to_str(err.errmsg))
        elif msg_code in riak.pb.messages.MESSAGE_CLASSES:
            msg = self._parse_msg(msg_code, self._inbuf[1:], is_ttb)
        else:
            raise Exception("unknown msg code %s" % msg_code)

        if expect and msg_code != expect:
            raise RiakError("unexpected protocol buffer message code: %d, %r"
                            % (msg_code, msg))
        logging.debug("pbc/connection received msg_code %d msg %s", msg_code, msg)
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

    def _parse_msg(self, code, packet, is_ttb=False):
        if is_ttb:
            if code != riak.pb.messages.MSG_CODE_TS_GET_RESP and \
               code != riak.pb.messages.MSG_CODE_TS_PUT_RESP:
                raise RiakError("TTB can't parse code: %d" % code)
            return erlastic.decode(packet)
        else:
            try:
                pbclass = riak.pb.messages.MESSAGE_CLASSES[code]
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
