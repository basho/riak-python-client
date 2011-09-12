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

import select, socket, struct

try:
    import json
except ImportError:
    import simplejson as json

from transport import RiakTransport
from riak.metadata import *
from riak.mapreduce import RiakMapReduce, RiakLink
from riak import RiakError

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


class RiakPbcTransport(RiakTransport):
    """
    The RiakPbcTransport object holds a connection to the protocol buffers interface
    on the riak server.
    """
    rw_names = {
        'default' : RIAKC_RW_DEFAULT,
        'all' : RIAKC_RW_ALL,
        'quorum' : RIAKC_RW_QUORUM,
        'one' : RIAKC_RW_ONE
        }
    def __init__(self, host='127.0.0.1', port=8087, client_id=None, timeout=None):
        """
        Construct a new RiakPbcTransport object.
        @param string host - Hostname or IP address (default '127.0.0.1')
        @param int port - Port number (default 8087)
        """
        if riakclient_pb2 is None:
            raise RiakError("this transport is not available (no protobuf)")

        super(RiakPbcTransport, self).__init__()
        self._host = host
        self._port = port
        self._client_id = client_id
        self._sock = None
        self._timeout = timeout

    def translate_rw_val(self, rw):
        val = self.rw_names.get(rw)
        if val is None:
            return rw
        return val

    def __copy__(self):
        return RiakPbcTransport(self._host, self._port)

    def ping(self):
        """
        Ping the remote server
        @return boolean
        """
        self.maybe_connect()
        self.send_msg_code(MSG_CODE_PING_REQ)
        msg_code, msg = self.recv_msg()
        if msg_code == MSG_CODE_PING_RESP:
            return 1
        else:
            return 0

    def get_client_id(self):
        """
        Get the client id used by this connection
        """
        self.maybe_connect()
        self.send_msg_code(MSG_CODE_GET_CLIENT_ID_REQ)
        msg_code, resp = self.recv_msg()
        if msg_code == MSG_CODE_GET_CLIENT_ID_RESP:
            return resp.client_id
        else:
            raise RiakError("unexpected protocol buffer message code: %d"%msg_code)

    def set_client_id(self, client_id):
        """
        Set the client id used by this connection
        """
        req = riakclient_pb2.RpbSetClientIdReq()
        req.client_id = client_id

        self.maybe_connect()
        self.send_msg(MSG_CODE_SET_CLIENT_ID_REQ, req)
        msg_code, resp = self.recv_msg()
        if msg_code == MSG_CODE_SET_CLIENT_ID_RESP:
            return True
        else:
            raise RiakError("unexpected protocol buffer message code: %d"%msg_code)

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

        self.maybe_connect()
        self.send_msg(MSG_CODE_GET_REQ, req)
        msg_code, resp = self.recv_msg()
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

        self.maybe_connect()
        self.send_msg(MSG_CODE_PUT_REQ, req)
        msg_code, resp = self.recv_msg()
        if msg_code != MSG_CODE_PUT_RESP:
            raise RiakError("unexpected protocol buffer message code: %d"%msg_code)
        if resp is not None:
            contents = []
            for c in resp.content:
                contents.append(self.decode_content(c))
            return resp.vclock, contents

    def delete(self, robj, rw = None):
        """
        Serialize get request and deserialize response
        """
        bucket = robj.get_bucket()

        req = riakclient_pb2.RpbDelReq()
        req.rw = self.translate_rw_val(rw)

        req.bucket = bucket.get_name()
        req.key = robj.get_key()

        self.maybe_connect()
        self.send_msg(MSG_CODE_DEL_REQ, req)
        msg_code, resp = self.recv_msg()
        if msg_code != MSG_CODE_DEL_RESP:
            raise RiakError("unexpected protocol buffer message code: %d"%msg_code)
        return self

    def get_keys(self, bucket):
        """
        Lists all keys within a bucket.
        """
        req = riakclient_pb2.RpbListKeysReq()
        req.bucket = bucket.get_name()

        self.maybe_connect()
        self.send_msg(MSG_CODE_LIST_KEYS_REQ, req)
        keys = []
        while True:
            msg_code, resp = self.recv_msg()
            if msg_code != MSG_CODE_LIST_KEYS_RESP:
                raise RiakError("unexpected protocol buffer message code: %d"%msg_code)

            for key in resp.keys:
                keys.append(key)

            if resp.HasField("done") and resp.done:
                break

        return keys

    def get_buckets(self):
        """
        Serialize bucket listing request and deserialize response
        """
        self.maybe_connect()
        self.send_msg_code(MSG_CODE_LIST_BUCKETS_REQ)
        msg_code, resp = self.recv_msg()
        if msg_code != MSG_CODE_LIST_BUCKETS_RESP:
          raise RiakError("unexpected protocol buffer message code: %d"%msg_code)
        return resp.buckets

    def get_bucket_props(self, bucket):
        """
        Serialize bucket property request and deserialize response
        """
        req = riakclient_pb2.RpbGetBucketReq()
        req.bucket = bucket.get_name()

        self.maybe_connect()
        self.send_msg(MSG_CODE_GET_BUCKET_REQ, req)
        msg_code, resp = self.recv_msg()
        if msg_code != MSG_CODE_GET_BUCKET_RESP:
            raise RiakError("unexpected protocol buffer message code: %d"%msg_code)
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

        self.maybe_connect()
        self.send_msg(MSG_CODE_SET_BUCKET_REQ, req)
        msg_code, resp = self.recv_msg()
        if msg_code != MSG_CODE_SET_BUCKET_RESP:
            raise RiakError("unexpected protocol buffer message code: %d"%msg_code)

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

        self.maybe_connect()
        self.send_msg(MSG_CODE_MAPRED_REQ, req)

        # dictionary of phase results - each content should be an encoded array
        # which is appended to the result for that phase.
        result = {}
        while True:
            msg_code, resp = self.recv_msg()
            if msg_code != MSG_CODE_MAPRED_RESP:
                raise RiakError("unexpected protocol buffer message code: %d"%msg_code)
            if resp.HasField("phase") and resp.HasField("response"):
                content = json.loads(resp.response)
                if resp.phase in result:
                    result[resp.phase] += content
                else:
                    result[resp.phase] = content

            if resp.HasField("done") and resp.done:
                break;

        # If a single result - return the same as the HTTP interface does
        # otherwise return all the phase information
        if not len(result):
            return None
        elif len(result) == 1:
            return result[max(result.keys())]
        else:
            return result


    def maybe_connect(self):
        if self._sock is None:
            self._sock = s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.settimeout(self._timeout)

            try:
                s.connect((self._host, self._port))
            except:
                self._sock = None
                raise

            if self._client_id:
                self.set_client_id(self._client_id)

    def send_msg_code(self, msg_code):
        pkt = struct.pack("!iB", 1, msg_code)
        self._sock.send(pkt)

    def encode_msg(self, msg_code, msg):
        str = msg.SerializeToString()
        slen = len(str)
        hdr = struct.pack("!iB", 1 + slen, msg_code)
        return hdr + str

    def send_msg(self, msg_code, msg):
        pkt = self.encode_msg(msg_code, msg)
        sent_len = self._sock.send(pkt)
        if sent_len != len(pkt):
            raise RiakError("PB socket returned short write %d - expected %d"%\
                            (sent_len, len(pkt)))

    def recv_msg(self):
        self.recv_pkt()
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
        return msg_code, msg

    def _recv(self, length):
        if self._timeout:
            ready = select.select([self._sock], [], [], self._timeout)
            if not ready[0]:
                raise socket.timeout("timed out")
        return self._sock.recv(length)

    def recv_pkt(self):
        nmsglen = self._recv(4)
        if len(nmsglen) != 4:
            self._sock = None
            raise RiakError("Socket returned short packet length %d - expected 4"%\
                            len(nmsglen))
        msglen, = struct.unpack('!i', nmsglen)
        self._inbuf_len = msglen
        self._inbuf = ''
        while len(self._inbuf) < msglen:
            want_len = min(8192, msglen - len(self._inbuf))
            recv_buf = self._recv(want_len)
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
    def __init__(self, host='127.0.0.1', port=8087, client_id=None, maxsize=0, block=False, timeout=None):
        if riakclient_pb2 is None:
            raise RiakError("this transport is not available (no protobuf)")

        self.host = host
        self.port = port
        self.client_id = client_id
        self.block = block
        self._timeout = timeout

        self.pool = Queue(maxsize)
        # Fill the queue up so that doing get() on it will block properly (check Queue#get)
        [self.pool.put(None) for _ in xrange(maxsize)]

    def _new_connection(self):
        """New PBC connection"""
        return RiakPbcTransport(self.host, self.port, self.client_id, timeout=self._timeout)

    def _get_connection(self):
        connection = None
        try:
            connection = self.pool.get(block=self.block, timeout=self._timeout)
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
