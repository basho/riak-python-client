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
from __future__ import with_statement

import errno
import socket
import struct

try:
    import json
except ImportError:
    import simplejson as json

from riak import RiakError
from riak.mapreduce import RiakLink
from riak.metadata import (
        MD_CHARSET,
        MD_CTYPE,
        MD_ENCODING,
        MD_INDEX,
        MD_LASTMOD,
        MD_LASTMOD_USECS,
        MD_LINKS,
        MD_USERMETA,
        MD_VTAG,
        )
from riak.riak_index_entry import RiakIndexEntry
from riak.transports import connection
from riak.transports.transport import RiakTransport
import riak.util

try:
    import riak_pb
except ImportError:
    riak_pb = None

## Protocol codes
MSG_CODE_ERROR_RESP = 0
MSG_CODE_PING_REQ = 1
MSG_CODE_PING_RESP = 2
MSG_CODE_GET_CLIENT_ID_REQ = 3
MSG_CODE_GET_CLIENT_ID_RESP = 4
MSG_CODE_SET_CLIENT_ID_REQ = 5
MSG_CODE_SET_CLIENT_ID_RESP = 6
MSG_CODE_GET_SERVER_INFO_REQ = 7
MSG_CODE_GET_SERVER_INFO_RESP = 8
MSG_CODE_GET_REQ = 9
MSG_CODE_GET_RESP = 10
MSG_CODE_PUT_REQ = 11
MSG_CODE_PUT_RESP = 12
MSG_CODE_DEL_REQ = 13
MSG_CODE_DEL_RESP = 14
MSG_CODE_LIST_BUCKETS_REQ = 15
MSG_CODE_LIST_BUCKETS_RESP = 16
MSG_CODE_LIST_KEYS_REQ = 17
MSG_CODE_LIST_KEYS_RESP = 18
MSG_CODE_GET_BUCKET_REQ = 19
MSG_CODE_GET_BUCKET_RESP = 20
MSG_CODE_SET_BUCKET_REQ = 21
MSG_CODE_SET_BUCKET_RESP = 22
MSG_CODE_MAPRED_REQ = 23
MSG_CODE_MAPRED_RESP = 24
MSG_CODE_INDEX_REQ = 25
MSG_CODE_INDEX_RESP = 26
MSG_CODE_SEARCH_QUERY_REQ = 27
MSG_CODE_SEARCH_QUERY_RESP = 28

RIAKC_RW_ONE = 4294967294
RIAKC_RW_QUORUM = 4294967293
RIAKC_RW_ALL = 4294967292
RIAKC_RW_DEFAULT = 4294967291

# These are a specific set of socket errors
# that could be raised on send/recv that indicate
# that the socket is closed or reset, and is not
# usable. On seeing any of these errors, the socket
# should be closed, and the connection re-established.
CONN_CLOSED_ERRORS = (
                        errno.EHOSTUNREACH,
                        errno.ECONNRESET,
                        errno.EBADF,
                        errno.EPIPE
                     )


class SocketWithId(connection.Socket):
    def __init__(self, host, port):
        super(SocketWithId, self).__init__(host, port)
        self.last_client_id = None

    def maybe_connect(self):
        # If we're going to establish a new connection, then reset the last
        # client_id used on this connection.
        if self.sock is None:
            self.last_client_id = None
            super(SocketWithId, self).maybe_connect()

    def send(self, pkt):
        try:
            self.sock.sendall(pkt)
        except socket.error, e:
            # If the socket is in a bad state, close it and allow it
            # to re-connect on the next try
            if e[0] in CONN_CLOSED_ERRORS:
                self.close()
            raise

    def recv(self, want_len):
        try:
            res = self.sock.recv(want_len)

            # Assume the socket is closed if no data is
            # returned on a blocking read.
            if len(res) == 0 and want_len > 0:
                self.close()

            return res

        except socket.error, e:
            # If the socket is in a bad state, close it and allow it
            # to re-connect on the next try
            if e[0] in CONN_CLOSED_ERRORS:
                self.close()
            raise


class RiakPbcTransport(RiakTransport):
    """
    The RiakPbcTransport object holds a connection to the protocol
    buffers interface on the riak server.
    """

    # We're using the new RiakTransport API
    api = 2

    rw_names = {
        'default': RIAKC_RW_DEFAULT,
        'all': RIAKC_RW_ALL,
        'quorum': RIAKC_RW_QUORUM,
        'one': RIAKC_RW_ONE
        }

    # The ConnectionManager class that this transport prefers.
    default_cm = connection.cm_using(SocketWithId)

    def __init__(self, cm, client_id=None, max_attempts=1, **unused_options):
        """
        Construct a new RiakPbcTransport object.
        """
        if riak_pb is None:
            raise RiakError("this transport is not available (no protobuf)")

        super(RiakPbcTransport, self).__init__()

        self._cm = cm
        self._client_id = client_id
        self._max_attempts = max_attempts

    # FeatureDetection API
    def _server_version(self):
        return self.get_server_info()['server_version']

    def translate_rw_val(self, rw):
        val = self.rw_names.get(rw)
        if val is None:
            return rw
        return val

    def __copy__(self):
        return RiakPbcTransport(self._cm, self._client_id)

    def ping(self):
        """
        Ping the remote server
        @return boolean
        """
        # An expected response code of None implies "any response is valid".
        msg_code, msg = self.send_msg_code(MSG_CODE_PING_REQ, None)
        if msg_code == MSG_CODE_PING_RESP:
            return 1
        else:
            return 0

    def get_server_info(self):
        """
        Get information about the server
        """
        msg_code, resp = self.send_msg_code(MSG_CODE_GET_SERVER_INFO_REQ,
                                            MSG_CODE_GET_SERVER_INFO_RESP)
        return {'node': resp.node, 'server_version': resp.server_version}

    def get_client_id(self):
        """
        Get the client id used by this connection
        """
        msg_code, resp = self.send_msg_code(MSG_CODE_GET_CLIENT_ID_REQ,
                                            MSG_CODE_GET_CLIENT_ID_RESP)
        return resp.client_id

    def set_client_id(self, client_id):
        """
        Set the client id used by this connection
        """
        req = riak_pb.RpbSetClientIdReq()
        req.client_id = client_id

        msg_code, resp = self.send_msg(MSG_CODE_SET_CLIENT_ID_REQ, req,
                                       MSG_CODE_SET_CLIENT_ID_RESP)

        # Using different client_id values across connections is a bad idea
        # since you never know which connection you might use for a given
        # API call. Setting the client_id manually (rather than as part of
        # the transport construction) can be error-prone since the connection
        # could drop and be reinstated using self._client_id.
        #
        # To minimize the potential impact of variant client_id values across
        # connections, we'll store this new client_id and use it for all
        # future connections.
        self._client_id = client_id

        return True

    def get(self, robj, r=None, pr=None, vtag=None):
        """
        Serialize get request and deserialize response
        """
        if vtag is not None:
            raise RiakError("PB transport does not support vtags")

        bucket = robj.get_bucket()

        req = riak_pb.RpbGetReq()
        req.r = self.translate_rw_val(r)
        if self.quorum_controls():
            req.pr = self.translate_rw_val(pr)

        if self.tombstone_vclocks():
            req.deletedvclock = 1

        req.bucket = bucket.get_name()
        req.key = robj.get_key()

        # An expected response code of None implies "any response is valid".
        msg_code, resp = self.send_msg(MSG_CODE_GET_REQ, req, None)
        if msg_code == MSG_CODE_GET_RESP:
            contents = []
            for c in resp.content:
                contents.append(self.decode_content(c))
            return resp.vclock, contents
        else:
            return None

    def put(self, robj, w=None, dw=None, pw=None, return_body=True,
            if_none_match=False):
        """
        Serialize get request and deserialize response
        """
        bucket = robj.get_bucket()

        req = riak_pb.RpbPutReq()
        req.w = self.translate_rw_val(w)
        req.dw = self.translate_rw_val(dw)
        if self.quorum_controls():
            req.pw = self.translate_rw_val(pw)

        if return_body:
            req.return_body = 1
        if if_none_match:
            req.if_none_match = 1

        req.bucket = bucket.get_name()
        req.key = robj.get_key()
        vclock = robj.vclock()
        if vclock:
            req.vclock = vclock

        self.pbify_content(robj.get_metadata(),
                           robj.get_encoded_data(),
                           req.content)

        msg_code, resp = self.send_msg(MSG_CODE_PUT_REQ, req,
                                       MSG_CODE_PUT_RESP)
        if resp is not None:
            contents = []
            for c in resp.content:
                contents.append(self.decode_content(c))
            return resp.vclock, contents

    def put_new(self, robj, w=None, dw=None, pw=None, return_body=True,
                if_none_match=False):
        """Put a new object into the Riak store, returning its (new) key.

        If return_meta is False, then the vlock and metadata return values
        will be None.

        @return (key, vclock, metadata)
        """
        # Note that this won't work on 0.14 nodes.
        bucket = robj.get_bucket()

        req = riak_pb.RpbPutReq()
        req.w = self.translate_rw_val(w)
        req.dw = self.translate_rw_val(dw)
        req.pw = self.translate_rw_val(pw)

        if return_body:
            req.return_body = 1
        if if_none_match:
            req.if_none_match = 1

        req.bucket = bucket.get_name()

        self.pbify_content(robj.get_metadata(),
                           robj.get_encoded_data(),
                           req.content)

        msg_code, resp = self.send_msg(MSG_CODE_PUT_REQ, req,
                                       MSG_CODE_PUT_RESP)
        if not resp:
            raise RiakError("missing response object")
        if len(resp.content) != 1:
            raise RiakError("siblings were returned from object creation")

        metadata, content = self.decode_content(resp.content[0])
        return resp.key, resp.vclock, metadata

    def delete(self, robj, rw=None, r=None, w=None, dw=None, pr=None, pw=None):
        """
        Serialize get request and deserialize response
        """
        bucket = robj.get_bucket()

        req = riak_pb.RpbDelReq()
        req.rw = self.translate_rw_val(rw)
        req.r = self.translate_rw_val(r)
        req.w = self.translate_rw_val(w)
        req.dw = self.translate_rw_val(dw)

        if self.quorum_controls():
            req.pr = self.translate_rw_val(pr)
            req.pw = self.translate_rw_val(pw)

        if self.tombstone_vclocks() and robj.vclock():
            req.vclock = robj.vclock()

        req.bucket = bucket.get_name()
        req.key = robj.get_key()

        msg_code, resp = self.send_msg(MSG_CODE_DEL_REQ, req,
                                       MSG_CODE_DEL_RESP)
        return self

    def get_keys(self, bucket):
        """
        Lists all keys within a bucket.
        """
        req = riak_pb.RpbListKeysReq()
        req.bucket = bucket.get_name()

        keys = []

        def _handle_response(resp):
            for key in resp.keys:
                keys.append(key)
        self.send_msg_multi(MSG_CODE_LIST_KEYS_REQ, req,
                            MSG_CODE_LIST_KEYS_RESP, _handle_response)

        return keys

    def get_buckets(self):
        """
        Serialize bucket listing request and deserialize response
        """
        msg_code, resp = self.send_msg_code(MSG_CODE_LIST_BUCKETS_REQ,
                                            MSG_CODE_LIST_BUCKETS_RESP)
        return resp.buckets

    def get_bucket_props(self, bucket):
        """
        Serialize bucket property request and deserialize response
        """
        req = riak_pb.RpbGetBucketReq()
        req.bucket = bucket.get_name()

        msg_code, resp = self.send_msg(MSG_CODE_GET_BUCKET_REQ, req,
                                       MSG_CODE_GET_BUCKET_RESP)
        props = {}
        if resp.props.HasField('n_val'):
            props['n_val'] = resp.props.n_val
        if resp.props.HasField('allow_mult'):
            props['allow_mult'] = resp.props.allow_mult

        return props

    def set_bucket_props(self, bucket, props):
        """
        Serialize set bucket property request and deserialize response
        """
        req = riak_pb.RpbSetBucketReq()
        req.bucket = bucket.get_name()
        if not 'n_val' in props and not 'allow_mult' in props:
            return self

        if 'n_val' in props:
            req.props.n_val = props['n_val']
        if 'allow_mult' in props:
            req.props.allow_mult = props['allow_mult']

        msg_code, resp = self.send_msg(MSG_CODE_SET_BUCKET_REQ, req,
                                       MSG_CODE_SET_BUCKET_RESP)
        return self

    def mapred(self, inputs, query, timeout=None):
        # Construct the job, optionally set the timeout...
        job = {'inputs': inputs, 'query': query}
        if timeout is not None:
            job['timeout'] = timeout

        content = json.dumps(job)

        req = riak_pb.RpbMapRedReq()
        req.request = content
        req.content_type = "application/json"

        # dictionary of phase results - each content should be an encoded array
        # which is appended to the result for that phase.
        result = {}

        def _handle_response(resp):
            if resp.HasField("phase") and resp.HasField("response"):
                content = json.loads(resp.response)
                if resp.phase in result:
                    result[resp.phase] += content
                else:
                    result[resp.phase] = content
        self.send_msg_multi(MSG_CODE_MAPRED_REQ, req, MSG_CODE_MAPRED_RESP,
                            _handle_response)

        # If a single result - return the same as the HTTP interface does
        # otherwise return all the phase information
        if not len(result):
            return None
        elif len(result) == 1:
            return result[max(result.keys())]
        else:
            return result

    def get_index(self, bucket, index, startkey, endkey=None):
        if not self.pb_indexes():
            return self._get_index_mapred_emu(bucket, index, startkey, endkey)

        req = riak_pb.RpbIndexReq(bucket=bucket, index=index)
        if endkey:
            req.qtype = riak_pb.RpbIndexReq.range
            req.range_min = str(startkey)
            req.range_max = str(endkey)
        else:
            req.qtype = riak_pb.RpbIndexReq.eq
            req.key = str(startkey)

        msg_code, resp = self.send_msg(MSG_CODE_INDEX_REQ, req,
                                       MSG_CODE_INDEX_RESP)
        return resp.keys

    def search(self, index, query, **params):
        if not self.pb_search():
            return self._search_mapred_emu(index, query)

        req = riak_pb.RpbSearchQueryReq(index=index, q=query)
        if 'rows' in params:
            req.rows = params['rows']
        if 'start' in params:
            req.start = params['start']
        if 'sort' in params:
            req.sort = params['sort']
        if 'filter' in params:
            req.filter = params['filter']
        if 'df' in params:
            req.df = params['df']
        if 'op' in params:
            req.op = params['op']
        if 'q.op' in params:
            req.op = params['q.op']
        if 'fl' in params:
            if isinstance(params['fl'], list):
                req.fl.extend(params['fl'])
            else:
                req.fl.append(params['fl'])
        if 'presort' in params:
            req.presort = params['presort']

        msg_code, resp = self.send_msg(MSG_CODE_SEARCH_QUERY_REQ, req,
                                       MSG_CODE_SEARCH_QUERY_RESP)

        result = {}
        if resp.HasField('max_score'):
            result['max_score'] = resp.max_score
        if resp.HasField('num_found'):
            result['num_found'] = resp.num_found
        docs = []
        for doc in resp.docs:
            resultdoc = {}
            for pair in doc.fields:
                resultdoc[pair.key] = pair.value
            docs.append(resultdoc)
        result['docs'] = docs
        return result

    def send_msg_code(self, msg_code, expect):
        with self._cm.withconn() as conn:
            self.send_pkt(conn, struct.pack("!iB", 1, msg_code))
            return self.recv_msg(conn, expect)

    def encode_msg(self, msg_code, msg):
        str = msg.SerializeToString()
        slen = len(str)
        hdr = struct.pack("!iB", 1 + slen, msg_code)
        return hdr + str

    def send_msg(self, msg_code, msg, expect):
        with self._cm.withconn() as conn:
            self.send_pkt(conn, self.encode_msg(msg_code, msg))
            if msg_code == MSG_CODE_SET_CLIENT_ID_REQ:
                conn.last_client_id = self._client_id
            return self.recv_msg(conn, expect)

    def send_msg_multi(self, msg_code, msg, expect, handler):
        with self._cm.withconn() as conn:
            self.send_pkt(conn, self.encode_msg(msg_code, msg))
            while True:
                msg_code, resp = self.recv_msg(conn, expect)
                handler(resp)
                if resp.HasField("done") and resp.done:
                    break

    def send_pkt(self, conn, pkt):
        attempt = 0
        e = None
        for attempt in xrange(self._max_attempts):
            e = None
            try:
                conn.maybe_connect()

                # If the last client_id used on this connection is
                # different than our client_id, then set a new ID on
                # the connection.
                if conn.last_client_id != self._client_id:
                    req = riak_pb.RpbSetClientIdReq()
                    req.client_id = self._client_id
                    conn.send(self.encode_msg(MSG_CODE_SET_CLIENT_ID_REQ, req))
                    conn.last_client_id = self._client_id
                    self.recv_msg(conn, MSG_CODE_SET_CLIENT_ID_RESP)

                conn.send(pkt)
                break
            except socket.error, e:
                # If this is some unknown socket error bail out
                # instead of retrying
                if e[0] not in CONN_CLOSED_ERRORS:
                    raise

        # Max attempts reached, raise whatever exception we are getting
        if attempt + 1 == self._max_attempts and e is not None:
            raise e

    def recv_msg(self, conn, expect):
        self.recv_pkt(conn)
        msg_code, = struct.unpack("B", self._inbuf[:1])
        if msg_code == MSG_CODE_ERROR_RESP:
            msg = riak_pb.RpbErrorResp()
            msg.ParseFromString(self._inbuf[1:])
            raise Exception(msg.errmsg)
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
            raise RiakError("unexpected protocol buffer message code: %d"
                            % msg_code)
        return msg_code, msg

    def recv_pkt(self, conn):
        nmsglen = conn.recv(4)
        if len(nmsglen) != 4:
            raise RiakError(
                "Socket returned short packet length %d - expected 4"
                % len(nmsglen))
        msglen, = struct.unpack('!i', nmsglen)
        self._inbuf_len = msglen
        self._inbuf = ''
        while len(self._inbuf) < msglen:
            want_len = min(8192, msglen - len(self._inbuf))
            recv_buf = conn.recv(want_len)
            if not recv_buf:
                break
            self._inbuf += recv_buf
        if len(self._inbuf) != self._inbuf_len:
            raise RiakError("Socket returned short packet %d - expected %d"
                            % (len(self._inbuf), self._inbuf_len))

    def decode_contents(self, rpb_contents):
        contents = []
        for rpb_c in rpb_contents:
            contents.append(self.decode_content(rpb_c))
        return contents

    def decode_content(self, rpb_content):
        metadata = {}
        if rpb_content.HasField("deleted"):
            metadata[MD_DELETED] = True
        if rpb_content.HasField("content_type"):
            metadata[MD_CTYPE] = rpb_content.content_type
        if rpb_content.HasField("charset"):
            metadata[MD_CHARSET] = rpb_content.charset
        if rpb_content.HasField("content_encoding"):
            metadata[MD_ENCODING] = rpb_content.content_encoding
        if rpb_content.HasField("vtag"):
            metadata[MD_VTAG] = rpb_content.vtag
        links = []
        for link in rpb_content.links:
            if link.HasField("bucket"):
                bucket = link.bucket
            else:
                bucket = None
            if link.HasField("key"):
                key = link.key
            else:
                key = None
            if link.HasField("tag"):
                tag = link.tag
            else:
                tag = None
            links.append(RiakLink(bucket, key, tag))
        if links:
            metadata[MD_LINKS] = links
        if rpb_content.HasField("last_mod"):
            metadata[MD_LASTMOD] = rpb_content.last_mod
        if rpb_content.HasField("last_mod_usecs"):
            metadata[MD_LASTMOD_USECS] = rpb_content.last_mod_usecs
        usermeta = {}
        for usermd in rpb_content.usermeta:
            usermeta[usermd.key] = usermd.value
        if len(usermeta) > 0:
            metadata[MD_USERMETA] = usermeta
        indexes = []
        for index in rpb_content.indexes:
            rie = RiakIndexEntry(index.key, index.value)
            indexes.append(rie)
        if len(indexes) > 0:
            metadata[MD_INDEX] = indexes
        return metadata, rpb_content.value

    def pbify_content(self, metadata, data, rpb_content):
        # Convert the broken out fields, building up
        # pbmetadata for any unknown ones
        for k, v in metadata.iteritems():
            if k == MD_CTYPE:
                rpb_content.content_type = v
            elif k == MD_CHARSET:
                rpb_content.charset = v
            elif k == MD_ENCODING:
                rpb_content.charset = v
            elif k == MD_USERMETA:
                for uk, uv in v.iteritems():
                    pair = rpb_content.usermeta.add()
                    pair.key = uk
                    pair.value = uv
            elif k == MD_INDEX:
                for rie in v:
                    pair = rpb_content.indexes.add()
                    pair.key = rie.get_field()
                    pair.value = rie.get_value()
            elif k == MD_LINKS:
                for link in v:
                    pb_link = rpb_content.links.add()
                    pb_link.bucket = link.get_bucket()
                    pb_link.key = link.get_key()
                    pb_link.tag = link.get_tag()
        rpb_content.value = data
