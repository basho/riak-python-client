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

import riak_pb
from riak import RiakError
from riak.transports.transport import RiakTransport
from connection import RiakPbcConnection
from stream import RiakPbcKeyStream, RiakPbcMapredStream
from codec import RiakPbcCodec

from messages import (
    MSG_CODE_PING_REQ,
    MSG_CODE_PING_RESP,
    MSG_CODE_GET_CLIENT_ID_REQ,
    MSG_CODE_GET_CLIENT_ID_RESP,
    MSG_CODE_SET_CLIENT_ID_REQ,
    MSG_CODE_SET_CLIENT_ID_RESP,
    MSG_CODE_GET_SERVER_INFO_REQ,
    MSG_CODE_GET_SERVER_INFO_RESP,
    MSG_CODE_GET_REQ,
    MSG_CODE_GET_RESP,
    MSG_CODE_PUT_REQ,
    MSG_CODE_PUT_RESP,
    MSG_CODE_DEL_REQ,
    MSG_CODE_DEL_RESP,
    MSG_CODE_LIST_BUCKETS_REQ,
    MSG_CODE_LIST_BUCKETS_RESP,
    MSG_CODE_LIST_KEYS_REQ,
    MSG_CODE_GET_BUCKET_REQ,
    MSG_CODE_GET_BUCKET_RESP,
    MSG_CODE_SET_BUCKET_REQ,
    MSG_CODE_SET_BUCKET_RESP,
    MSG_CODE_MAPRED_REQ,
    MSG_CODE_INDEX_REQ,
    MSG_CODE_INDEX_RESP,
    MSG_CODE_SEARCH_QUERY_REQ,
    MSG_CODE_SEARCH_QUERY_RESP
)


class RiakPbcTransport(RiakTransport, RiakPbcConnection, RiakPbcCodec):
    """
    The RiakPbcTransport object holds a connection to the protocol
    buffers interface on the riak server.
    """

    def __init__(self, node=None, client=None, timeout=None, *unused_options):
        """
        Construct a new RiakPbcTransport object.
        """
        super(RiakPbcTransport, self).__init__()

        self._client = client
        self._node = node
        self._address = (node.host, node.pb_port)
        self._timeout = timeout
        self._connect()

    # FeatureDetection API
    def _server_version(self):
        return self.get_server_info()['server_version']

    def ping(self):
        """
        Ping the remote server
        """

        msg_code, msg = self._request(MSG_CODE_PING_REQ)
        if msg_code == MSG_CODE_PING_RESP:
            return True
        else:
            return False

    def get_server_info(self):
        """
        Get information about the server
        """
        msg_code, resp = self._request(MSG_CODE_GET_SERVER_INFO_REQ,
                                       expect=MSG_CODE_GET_SERVER_INFO_RESP)
        return {'node': resp.node, 'server_version': resp.server_version}

    def _get_client_id(self):
        msg_code, resp = self._request(MSG_CODE_GET_CLIENT_ID_REQ,
                                       expect=MSG_CODE_GET_CLIENT_ID_RESP)
        return resp.client_id

    def _set_client_id(self, client_id):
        req = riak_pb.RpbSetClientIdReq()
        req.client_id = client_id

        msg_code, resp = self._request(MSG_CODE_SET_CLIENT_ID_REQ, req,
                                       MSG_CODE_SET_CLIENT_ID_RESP)

        self._client_id = client_id

    client_id = property(_get_client_id, _set_client_id,
                         doc="""the client ID for this connection""")

    def get(self, robj, r=None, pr=None):
        """
        Serialize get request and deserialize response
        """
        bucket = robj.bucket

        req = riak_pb.RpbGetReq()
        if r:
            req.r = self.translate_rw_val(r)
        if self.quorum_controls() and pr:
            req.pr = self.translate_rw_val(pr)

        if self.tombstone_vclocks():
            req.deletedvclock = 1

        req.bucket = bucket.name
        req.key = robj.key

        msg_code, resp = self._request(MSG_CODE_GET_REQ, req)
        if msg_code == MSG_CODE_GET_RESP:
            return self._decoded_contents(resp, robj)
        else:
            return None

    def put(self, robj, w=None, dw=None, pw=None, return_body=True,
            if_none_match=False):
        """
        Serialize get request and deserialize response
        """
        bucket = robj.bucket

        req = riak_pb.RpbPutReq()
        if w:
            req.w = self.translate_rw_val(w)
        if dw:
            req.dw = self.translate_rw_val(dw)
        if self.quorum_controls() and pw:
            req.pw = self.translate_rw_val(pw)

        if return_body:
            req.return_body = 1
        if if_none_match:
            req.if_none_match = 1

        req.bucket = bucket.name
        if robj.key:
            req.key = robj.key
        if robj.vclock:
            req.vclock = robj.vclock.encode('binary')

        self._encode_content(robj, req.content)

        msg_code, resp = self._request(MSG_CODE_PUT_REQ, req,
                                       MSG_CODE_PUT_RESP)

        if resp is not None:
            return self._decoded_contents(resp, robj)
        elif not robj.key:
            raise RiakError("missing response object")
        else:
            return robj

    put_new = put

    def delete(self, robj, rw=None, r=None, w=None, dw=None, pr=None, pw=None):
        """
        Serialize get request and deserialize response
        """
        bucket = robj.bucket

        req = riak_pb.RpbDelReq()
        if rw:
            req.rw = self.translate_rw_val(rw)
        if r:
            req.r = self.translate_rw_val(r)
        if w:
            req.w = self.translate_rw_val(w)
        if dw:
            req.dw = self.translate_rw_val(dw)

        if self.quorum_controls():
            if pr:
                req.pr = self.translate_rw_val(pr)
            if pw:
                req.pw = self.translate_rw_val(pw)

        if self.tombstone_vclocks() and robj.vclock:
            req.vclock = robj.vclock.encode('binary')

        req.bucket = bucket.name
        req.key = robj.key

        msg_code, resp = self._request(MSG_CODE_DEL_REQ, req,
                                       MSG_CODE_DEL_RESP)
        return self

    def get_keys(self, bucket):
        """
        Lists all keys within a bucket.
        """
        keys = []
        for keylist in self.stream_keys(bucket):
            for key in keylist:
                keys.append(key)

        return keys

    def stream_keys(self, bucket):
        """
        Streams keys from a bucket, returning an iterator that yields
        lists of keys.
        """
        req = riak_pb.RpbListKeysReq()
        req.bucket = bucket.name

        self._send_msg(MSG_CODE_LIST_KEYS_REQ, req)

        return RiakPbcKeyStream(self)

    def get_buckets(self):
        """
        Serialize bucket listing request and deserialize response
        """
        msg_code, resp = self._request(MSG_CODE_LIST_BUCKETS_REQ,
                                       expect=MSG_CODE_LIST_BUCKETS_RESP)
        return resp.buckets

    def get_bucket_props(self, bucket):
        """
        Serialize bucket property request and deserialize response
        """
        req = riak_pb.RpbGetBucketReq()
        req.bucket = bucket.name

        msg_code, resp = self._request(MSG_CODE_GET_BUCKET_REQ, req,
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
        req.bucket = bucket.name
        for key in props:
            if key not in ['n_val', 'allow_mult']:
                raise NotImplementedError

        if 'n_val' in props:
            req.props.n_val = props['n_val']
        if 'allow_mult' in props:
            req.props.allow_mult = props['allow_mult']

        msg_code, resp = self._request(MSG_CODE_SET_BUCKET_REQ, req,
                                       MSG_CODE_SET_BUCKET_RESP)
        return self

    def mapred(self, inputs, query, timeout=None):
        # dictionary of phase results - each content should be an encoded array
        # which is appended to the result for that phase.
        result = {}
        for phase, content in self.stream_mapred(inputs, query, timeout):
            if phase in result:
                result[phase] += content
            else:
                result[phase] = content

        # If a single result - return the same as the HTTP interface does
        # otherwise return all the phase information
        if not len(result):
            return None
        elif len(result) == 1:
            return result[max(result.keys())]
        else:
            return result

    def stream_mapred(self, inputs, query, timeout=None):
        # Construct the job, optionally set the timeout...
        content = self._construct_mapred_json(inputs, query, timeout)

        req = riak_pb.RpbMapRedReq()
        req.request = content
        req.content_type = "application/json"

        self._send_msg(MSG_CODE_MAPRED_REQ, req)

        return RiakPbcMapredStream(self)

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

        msg_code, resp = self._request(MSG_CODE_INDEX_REQ, req,
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

        msg_code, resp = self._request(MSG_CODE_SEARCH_QUERY_REQ, req,
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
                ukey = unicode(pair.key, 'utf-8')
                uval = unicode(pair.value, 'utf-8')
                resultdoc[ukey] = uval
            docs.append(resultdoc)
        result['docs'] = docs
        return result
