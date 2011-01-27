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
import socket, struct

try:
    import json
except ImportError:
    import simplejson as json

from transport import RiakTransport
from riak.metadata import *
from riak.mapreduce import RiakMapReduce, RiakLink
from riak import RiakError
import riakclient_pb2

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
    def __init__(self, host='127.0.0.1', port=8087, client_id=None):
        """
        Construct a new RiakPbcTransport object.
        @param string host - Hostname or IP address (default '127.0.0.1')
        @param int port - Port number (default 8087)
        """
        super(RiakPbcTransport, self).__init__()
        self._host = host
        self._port = port
        self._client_id = client_id
        self._sock = None

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
            raise RiakError("unexpected protocol buffer message code: ", msg_code)

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
            raise RiakError("unexpected protocol buffer message code: ", msg_code)

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
            return (resp.vclock, contents)
        else:
            return 0

        return 0

    def put(self, robj, w = None, dw = None, return_body = True):
        """
        Serialize get request and deserialize response
        """
        bucket = robj.get_bucket()
        
        req = riakclient_pb2.RpbPutReq()
        req.w = self.translate_rw_val(w)
        req.dw = self.translate_rw_val(dw)
        if return_body == True:
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
            raise RiakError("unexpected protocol buffer message code: ", msg_code)
        if resp is not None:
            contents = []
            for c in resp.content:
                contents.append(self.decode_content(c))
            return (resp.vclock, contents)

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
            raise RiakError("unexpected protocol buffer message code: ", msg_code)
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
                raise RiakError("unexpected protocol buffer message code: ", msg_code)

            for key in resp.keys:
                keys.append(key)

            if resp.HasField("done") and resp.done:
                break;

        return keys

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
            raise RiakError("unexpected protocol buffer message code: ", msg_code)
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
        if 'n_val' in props:
            req.props.n_val = props['n_val']
        if 'allow_mult' in props:
            req.props.allow_mult = props['allow_mult']

        self.maybe_connect()
        self.send_msg(MSG_CODE_SET_BUCKET_REQ, req)
        msg_code, resp = self.recv_msg()
        if msg_code != MSG_CODE_SET_BUCKET_RESP:
            raise RiakError("unexpected protocol buffer message code: ", msg_code)

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
                raise RiakError("unexpected protocol buffer message code: ",
                                msg_code)
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
        if len(result) == 0:
            return None
        elif len(result) == 1:
            return result[max(result.keys())]
        else:
            return result


    def maybe_connect(self):
        if self._sock is None:
            self._sock = s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self._host, self._port))
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
            raise RiakError("PB socket returned short write {0} - expected {1}".
                            format(sent_len, len(pkt)))

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
        elif msg_code == MSG_CODE_GET_BUCKET_RESP:
            msg = riakclient_pb2.RpbGetBucketResp()
            msg.ParseFromString(self._inbuf[1:])
        elif msg_code == MSG_CODE_SET_BUCKET_RESP:
            msg = None
        elif msg_code == MSG_CODE_MAPRED_RESP:
            msg = riakclient_pb2.RpbMapRedResp()
            msg.ParseFromString(self._inbuf[1:])
        else:
            raise Exception("unknown msg code {0}".format(msg_code))
        return msg_code, msg


    def recv_pkt(self):
        nmsglen = self._sock.recv(4)
        if (len(nmsglen) != 4):
            raise RiakError("Socket returned short packet length {0} - expected 4".
                            format(nmsglen))
        msglen, = struct.unpack('!i', nmsglen)
        self._inbuf_len = msglen
        self._inbuf = ''
        while len(self._inbuf) < msglen:
            want_len = min(8192, msglen - len(self._inbuf))
            recv_buf = self._sock.recv(want_len)
            if not recv_buf: break
            self._inbuf += recv_buf
        if len(self._inbuf) != self._inbuf_len:
            raise RiakError("Socket returned short packet {0} - expected {1}".
                            format(len(self._inbuf), self._inbuf_len))

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
        if links != []:
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
        return (metadata, rpb_content.value)

    def pbify_content(self, metadata, data, rpb_content) :
        pbmetadata = {}
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
                for uk, uv in v:
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
