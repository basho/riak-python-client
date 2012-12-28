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
from riak import RiakError
from messages import (
    MSG_CODE_ERROR_RESP,
    # MSG_CODE_PING_REQ,
    MSG_CODE_PING_RESP,
    # MSG_CODE_GET_CLIENT_ID_REQ,
    MSG_CODE_GET_CLIENT_ID_RESP,
    # MSG_CODE_SET_CLIENT_ID_REQ,
    MSG_CODE_SET_CLIENT_ID_RESP,
    # MSG_CODE_GET_SERVER_INFO_REQ,
    MSG_CODE_GET_SERVER_INFO_RESP,
    # MSG_CODE_GET_REQ,
    MSG_CODE_GET_RESP,
    # MSG_CODE_PUT_REQ,
    MSG_CODE_PUT_RESP,
    # MSG_CODE_DEL_REQ,
    MSG_CODE_DEL_RESP,
    # MSG_CODE_LIST_BUCKETS_REQ,
    MSG_CODE_LIST_BUCKETS_RESP,
    # MSG_CODE_LIST_KEYS_REQ,
    MSG_CODE_LIST_KEYS_RESP,
    # MSG_CODE_GET_BUCKET_REQ,
    MSG_CODE_GET_BUCKET_RESP,
    # MSG_CODE_SET_BUCKET_REQ,
    MSG_CODE_SET_BUCKET_RESP,
    # MSG_CODE_MAPRED_REQ,
    MSG_CODE_MAPRED_RESP,
    # MSG_CODE_INDEX_REQ,
    MSG_CODE_INDEX_RESP,
    # MSG_CODE_SEARCH_QUERY_REQ,
    MSG_CODE_SEARCH_QUERY_RESP
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
        self._socket.send(self._encode_msg(msg_code, msg))

    def _recv_msg(self, expect=None):
        self._recv_pkt()
        msg_code, = struct.unpack("B", self._inbuf[:1])
        if msg_code == MSG_CODE_ERROR_RESP:
            err = riak_pb.RpbErrorResp()
            err.ParseFromString(self._inbuf[1:])
            raise RiakError(err.errmsg)
        elif msg_code == MSG_CODE_PING_RESP:
            msg = None
        elif msg_code == MSG_CODE_GET_SERVER_INFO_RESP:
            msg = riak_pb.RpbGetServerInfoResp()
            msg.ParseFromString(self._inbuf[1:])
        elif msg_code == MSG_CODE_GET_CLIENT_ID_RESP:
            msg = riak_pb.RpbGetClientIdResp()
            msg.ParseFromString(self._inbuf[1:])
        elif msg_code == MSG_CODE_SET_CLIENT_ID_RESP:
            msg = None
        elif msg_code == MSG_CODE_GET_RESP:
            msg = riak_pb.RpbGetResp()
            msg.ParseFromString(self._inbuf[1:])
        elif msg_code == MSG_CODE_PUT_RESP:
            msg = riak_pb.RpbPutResp()
            msg.ParseFromString(self._inbuf[1:])
        elif msg_code == MSG_CODE_DEL_RESP:
            msg = None
        elif msg_code == MSG_CODE_LIST_KEYS_RESP:
            msg = riak_pb.RpbListKeysResp()
            msg.ParseFromString(self._inbuf[1:])
        elif msg_code == MSG_CODE_LIST_BUCKETS_RESP:
            msg = riak_pb.RpbListBucketsResp()
            msg.ParseFromString(self._inbuf[1:])
        elif msg_code == MSG_CODE_GET_BUCKET_RESP:
            msg = riak_pb.RpbGetBucketResp()
            msg.ParseFromString(self._inbuf[1:])
        elif msg_code == MSG_CODE_SET_BUCKET_RESP:
            msg = None
        elif msg_code == MSG_CODE_MAPRED_RESP:
            msg = riak_pb.RpbMapRedResp()
            msg.ParseFromString(self._inbuf[1:])
        elif msg_code == MSG_CODE_INDEX_RESP:
            msg = riak_pb.RpbIndexResp()
            msg.ParseFromString(self._inbuf[1:])
        elif msg_code == MSG_CODE_SEARCH_QUERY_RESP:
            msg = riak_pb.RpbSearchQueryResp()
            msg.ParseFromString(self._inbuf[1:])
        else:
            raise Exception("unknown msg code %s" % msg_code)
        if expect and msg_code != expect:
            raise RiakError("unexpected protocol buffer message code: %d, %s"
                            % (msg_code, repr(msg)))
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
        self._socket = socket.create_connection(self._address,
                                                self._timeouts['connect'])

    def close(self):
        self._socket.shutdown(socket.SHUT_RDWR)

    # These are set in the RiakPbcTransport initializer
    _address = None
    _timeouts = {}
