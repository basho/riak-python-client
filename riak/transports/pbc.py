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

import socket, struct

try:
    import json
except ImportError:
    import simplejson as json

from riak.transports.transport import RiakTransport
from riak.metadata import *
from riak.mapreduce import RiakMapReduce, RiakLink
from riak import RiakError
from riak.riak_index_entry import RiakIndexEntry
from riak.transports import connection

try:
    import riakclient_pb2
except ImportError:
    riakclient_pb2 = None

## Protocol codes
MSG_CODE_ERROR_RESP           =  0
MSG_CODE_PING_REQ             =  1
MSG_CODE_PING_RESP            =  2
MSG_CODE_GET_CLIENT_ID_REQ    =  3
MSG_CODE_GET_CLIENT_ID_RESP   =  4
MSG_CODE_SET_CLIENT_ID_REQ    =  5
MSG_CODE_SET_CLIENT_ID_RESP   =  6
MSG_CODE_GET_SERVER_INFO_REQ  =  7
MSG_CODE_GET_SERVER_INFO_RESP =  8
MSG_CODE_GET_REQ              =  9
MSG_CODE_GET_RESP             = 10
MSG_CODE_PUT_REQ              = 11
MSG_CODE_PUT_RESP             = 12
MSG_CODE_DEL_REQ              = 13
MSG_CODE_DEL_RESP             = 14
MSG_CODE_LIST_BUCKETS_REQ     = 15
MSG_CODE_LIST_BUCKETS_RESP    = 16
MSG_CODE_LIST_KEYS_REQ        = 17
MSG_CODE_LIST_KEYS_RESP       = 18
MSG_CODE_GET_BUCKET_REQ       = 19
MSG_CODE_GET_BUCKET_RESP      = 20
MSG_CODE_SET_BUCKET_REQ       = 21
MSG_CODE_SET_BUCKET_RESP      = 22
MSG_CODE_MAPRED_REQ           = 23
MSG_CODE_MAPRED_RESP          = 24

RIAKC_RW_ONE = 4294967294
RIAKC_RW_QUORUM = 4294967293
RIAKC_RW_ALL = 4294967292
RIAKC_RW_DEFAULT = 4294967291


class SocketWithId(connection.Socket):
    def __init__(self, host, port):
        connection.Socket.__init__(self, host, port)
        self.last_client_id = None

    def maybe_connect(self):
        # If we're going to establish a new connection, then reset the last
        # client_id used on this connection.
        if self.sock is None:
            self.last_client_id = None

            connection.Socket.maybe_connect(self)

    def send(self, pkt):
        self.sock.sendall(pkt)

    def recv(self, want_len):
        return self.sock.recv(want_len)


class RiakPbcTransport(RiakTransport):
    """
    The RiakPbcTransport object holds a connection to the protocol buffers interface
    on the riak server.
    """

    # We're using the new RiakTransport API
    api = 2

    rw_names = {
        'default' : RIAKC_RW_DEFAULT,
        'all' : RIAKC_RW_ALL,
        'quorum' : RIAKC_RW_QUORUM,
        'one' : RIAKC_RW_ONE
        }

    # The ConnectionManager class that this transport prefers.
    default_cm = connection.cm_using(SocketWithId)

    def __init__(self, cm, client_id=None, **unused_options):
        """
        Construct a new RiakPbcTransport object.
        """
        if riakclient_pb2 is None:
            raise RiakError("this transport is not available (no protobuf)")

        super(RiakPbcTransport, self).__init__()

        self._cm = cm
        self._client_id = client_id

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
        req = riakclient_pb2.RpbSetClientIdReq()
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

    def get(self, robj, r = None, vtag = None):
        """
        Serialize get request and deserialize response
        """
        if vtag is not None:
            raise RiakError("PB transport does not support vtags")

        bucket = robj.get_bucket()

        req = riakclient_pb2.RpbGetReq()
        req.r = self.translate_rw_val(r)

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
            return 0

    def put(self, robj, w = None, dw = None, return_body = True):
        """
        Serialize get request and deserialize response
        """
        bucket = robj.get_bucket()

        req = riakclient_pb2.RpbPutReq()
        req.w = self.translate_rw_val(w)
        req.dw = self.translate_rw_val(dw)
        if return_body:
            req.return_body = 1

        req.bucket = bucket.get_name()
        req.key = robj.get_key()
        vclock = robj.vclock()
        if vclock is not None:
            req.vclock = vclock

        self.pbify_content(robj.get_metadata(), robj.get_encoded_data(), req.content)

        msg_code, resp = self.send_msg(MSG_CODE_PUT_REQ, req,
                                       MSG_CODE_PUT_RESP)
        if resp is not None:
            contents = []
            for c in resp.content:
                contents.append(self.decode_content(c))
            return resp.vclock, contents

    def put_new(self, robj, w=None, dw=None, return_meta=True):
        """Put a new object into the Riak store, returning its (new) key.

        If return_meta is False, then the vlock and metadata return values
        will be None.

        @return (key, vclock, metadata)
        """
        bucket = robj.get_bucket()

        req = riakclient_pb2.RpbPutReq()
        req.w = self.translate_rw_val(w)
        req.dw = self.translate_rw_val(dw)
        if return_meta:
            req.return_body = 1

        req.bucket = bucket.get_name()

        self.pbify_content(robj.get_metadata(), robj.get_encoded_data(), req.content)

        msg_code, resp = self.send_msg(MSG_CODE_PUT_REQ, req,
                                       MSG_CODE_PUT_RESP)
        if not resp:
            raise RiakError("missing response object")
        if len(resp.content) != 1:
            raise RiakError("siblings were returned from object creation")

        metadata, content = self.decode_content(resp.content[0])
        return resp.key, resp.vclock, metadata

    def delete(self, robj, rw = None):
        """
        Serialize get request and deserialize response
        """
        bucket = robj.get_bucket()

        req = riakclient_pb2.RpbDelReq()
        req.rw = self.translate_rw_val(rw)

        req.bucket = bucket.get_name()
        req.key = robj.get_key()

        msg_code, resp = self.send_msg(MSG_CODE_DEL_REQ, req,
                                       MSG_CODE_DEL_RESP)
        return self

    def get_keys(self, bucket):
        """
        Lists all keys within a bucket.
        """
        req = riakclient_pb2.RpbListKeysReq()
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
        req = riakclient_pb2.RpbGetBucketReq()
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
        req = riakclient_pb2.RpbSetBucketReq()
        req.bucket = bucket.get_name()
        if not 'n_val' in props and not 'allow_mult' in props: return self

        if 'n_val' in props:
            req.props.n_val = props['n_val']
        if 'allow_mult' in props:
            req.props.allow_mult = props['allow_mult']

        msg_code, resp = self.send_msg(MSG_CODE_SET_BUCKET_REQ, req,
                                       MSG_CODE_SET_BUCKET_RESP)
        return self

    def mapred(self, inputs, query, timeout=None):
        # Construct the job, optionally set the timeout...
        job = {'inputs':inputs, 'query':query}
        if timeout is not None:
            job['timeout'] = timeout

        content = json.dumps(job)

        req = riakclient_pb2.RpbMapRedReq()
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
        conn.maybe_connect()

        # If the last client_id used on this connection is different than our
        # client_id, then set a new ID on the connection.
        if conn.last_client_id != self._client_id:
            req = riakclient_pb2.RpbSetClientIdReq()
            req.client_id = self._client_id
            conn.send(self.encode_msg(MSG_CODE_SET_CLIENT_ID_REQ, req))
            conn.last_client_id = self._client_id
            self.recv_msg(conn, MSG_CODE_SET_CLIENT_ID_RESP)

        conn.send(pkt)

    def recv_msg(self, conn, expect):
        self.recv_pkt(conn)
        msg_code, = struct.unpack("B", self._inbuf[:1])
        if msg_code == MSG_CODE_ERROR_RESP:
            msg = riakclient_pb2.RpbErrorResp()
            msg.ParseFromString(self._inbuf[1:])
            raise Exception(msg.errmsg)
        elif msg_code == MSG_CODE_PING_RESP:
            msg = None
        elif msg_code == MSG_CODE_GET_CLIENT_ID_RESP:
            msg = riakclient_pb2.RpbGetClientIdResp()
            msg.ParseFromString(self._inbuf[1:])
        elif msg_code == MSG_CODE_SET_CLIENT_ID_RESP:
            msg = None
        elif msg_code == MSG_CODE_GET_RESP:
            msg = riakclient_pb2.RpbGetResp()
            msg.ParseFromString(self._inbuf[1:])
        elif msg_code == MSG_CODE_PUT_RESP:
            msg = riakclient_pb2.RpbPutResp()
            msg.ParseFromString(self._inbuf[1:])
        elif msg_code == MSG_CODE_DEL_RESP:
            msg = None
        elif msg_code == MSG_CODE_LIST_KEYS_RESP:
            msg = riakclient_pb2.RpbListKeysResp()
            msg.ParseFromString(self._inbuf[1:])
        elif msg_code == MSG_CODE_LIST_BUCKETS_RESP:
            msg = riakclient_pb2.RpbListBucketsResp()
            msg.ParseFromString(self._inbuf[1:])
        elif msg_code == MSG_CODE_GET_BUCKET_RESP:
            msg = riakclient_pb2.RpbGetBucketResp()
            msg.ParseFromString(self._inbuf[1:])
        elif msg_code == MSG_CODE_SET_BUCKET_RESP:
            msg = None
        elif msg_code == MSG_CODE_MAPRED_RESP:
            msg = riakclient_pb2.RpbMapRedResp()
            msg.ParseFromString(self._inbuf[1:])
        else:
            raise Exception("unknown msg code %s"%msg_code)
        if expect and msg_code != expect:
            raise RiakError("unexpected protocol buffer message code: %d"
                            % msg_code)
        return msg_code, msg


    def recv_pkt(self, conn):
        nmsglen = conn.recv(4)
        if len(nmsglen) != 4:
            raise RiakError("Socket returned short packet length %d - expected 4"%\
                            len(nmsglen))
        msglen, = struct.unpack('!i', nmsglen)
        self._inbuf_len = msglen
        self._inbuf = ''
        while len(self._inbuf) < msglen:
            want_len = min(8192, msglen - len(self._inbuf))
            recv_buf = conn.recv(want_len)
            if not recv_buf: break
            self._inbuf += recv_buf
        if len(self._inbuf) != self._inbuf_len:
            raise RiakError("Socket returned short packet %d - expected %d"%\
                            (len(self._inbuf), self._inbuf_len))

    def decode_contents(self, rpb_contents):
        contents = []
        for rpb_c in rpb_contents:
            contents.append(self.decode_content(rpb_c))
        return contents

    def decode_content(self, rpb_content):
        metadata = {}
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

    def pbify_content(self, metadata, data, rpb_content) :
        # Convert the broken out fields, building up
        # pbmetadata for any unknown ones
        for k,v in metadata.iteritems():
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

from Queue import Empty, Full, Queue
import contextlib
class RiakPbcCachedTransport(RiakTransport):
    """Threadsafe pool of PBC connections, based on urllib3's pool [aka Queue]"""

    # We're using the new RiakTransport API
    api = 2

    # The ConnectionManager class that this transport prefers.
    default_cm = connection.cm_using(SocketWithId)

    def __init__(self, cm,
                 client_id=None, maxsize=0, block=False, timeout=None,
                 **unused_options):
        if riakclient_pb2 is None:
            raise RiakError("this transport is not available (no protobuf)")

        ### backwards compat. we don't use the ConnectionManager (yet).
        self._cm = cm

        self.client_id = client_id
        self.block = block
        self.timeout = timeout

        self.pool = Queue(maxsize)
        # Fill the queue up so that doing get() on it will block properly (check Queue#get)
        [self.pool.put(None) for _ in xrange(maxsize)]

    def _new_connection(self):
        """New PBC connection"""
        return RiakPbcTransport(self._cm, self.client_id)

    def _get_connection(self):
        connection = None
        try:
            connection = self.pool.get(block=self.block, timeout=self.timeout)
        except Empty:
            pass
        return connection or self._new_connection()

    def _put_connection(self, connection):
        try:
            self.pool.put(connection, block=False)
        except Full:
            pass

    @contextlib.contextmanager
    def _get_connection_from_pool(self):
        """checkout conn, try operation, put conn back in pool"""
        connection = self._get_connection()
        try:
            yield connection
        finally:
            self._put_connection(connection)

    def ping(self):
        """
        Ping the remote server
        @return boolean
        """
        with self._get_connection_from_pool() as connection:
            return connection.ping()

    def get(self, robj, r = None, vtag = None):
        """
        Serialize get request and deserialize response
        @return (vclock=None, [(metadata, value)]=None)
        """
        with self._get_connection_from_pool() as connection:
            return connection.get(robj, r, vtag)

    def put(self, robj, w = None, dw = None, return_body = True):
        """
        Serialize put request and deserialize response - if 'content'
        is true, retrieve the updated metadata/content
        @return (vclock=None, [(metadata, value)]=None)
        """
        with self._get_connection_from_pool() as connection:
            return connection.put(robj, w, dw, return_body)

    def put_new(self, robj, w=None, dw=None, return_meta=True):
        """Put a new object into the Riak store, returning its (new) key.

        If return_meta is False, then the vlock and metadata return values
        will be None.

        @return (key, vclock, metadata)
        """
        with self._get_connection_from_pool() as connection:
            return connection.put_new(robj, w, dw, return_meta)

    def delete(self, robj, rw = None):
        """
        Serialize delete request and deserialize response
        @return true
        """
        with self._get_connection_from_pool() as connection:
            return connection.delete(robj, rw)

    def get_buckets(self):
        """
        Serialize bucket listing request and deserialize response
        """
        with self._get_connection_from_pool() as connection:
            return connection.get_buckets()

    def get_bucket_props(self, bucket) :
        """
        Serialize get bucket property request and deserialize response
        @return dict()
        """
        with self._get_connection_from_pool() as connection:
            return connection.get_bucket_props(bucket)

    def set_bucket_props(self, bucket, props) :
        """
        Serialize set bucket property request and deserialize response
        bucket = bucket object
        props = dictionary of properties
        @return boolean
        """
        with self._get_connection_from_pool() as connection:
            return connection.set_bucket_props(bucket, props)

    def mapred(self, inputs, query, timeout = None) :
        """
        Serialize map/reduce request
        """
        with self._get_connection_from_pool() as connection:
            return connection.mapred(inputs, query, timeout)

    def set_client_id(self, client_id):
        """Mmm, this can turn ugly if you use different id for different objects in the pool"""
        with self._get_connection_from_pool() as connection:
            return connection.set_client_id(client_id)

    def get_client_id(self):
        """see set_client_id notes, you can do wrong with this"""
        with self._get_connection_from_pool() as connection:
            return connection.get_client_id()
