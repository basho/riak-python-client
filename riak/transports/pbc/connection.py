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
from riak import RiakError
from messages import (
    MESSAGE_CLASSES,
    MSG_CODE_ERROR_RESP
)


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

    def _send_msg(self, msg_code, msg):
        self._connect()
        self._socket.send(self._encode_msg(msg_code, msg))

    def _recv_msg(self, expect=None):
        self._recv_pkt()
        msg_code, = struct.unpack("B", self._inbuf[:1])

        if msg_code is MSG_CODE_ERROR_RESP:
            err = self._parse_msg(msg_code, self._inbuf[1:])
            raise RiakError(err.errmsg)
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
        if len(nmsglen) != 4:
            raise RiakError(
                "Socket returned short packet length %d - expected 4"
                % len(nmsglen))
        msglen, = struct.unpack('!i', nmsglen)
        self._inbuf_len = msglen
        self._inbuf = ''
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

    def close(self):
        """
        Closes the underlying socket of the PB connection.
        """
        if self._socket:
            self._socket.shutdown(socket.SHUT_RDWR)

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
