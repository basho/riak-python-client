import riak.pb.messages
import riak.pb.riak_pb2
import riak.pb.riak_kv_pb2
import riak.pb.riak_ts_pb2

from riak import RiakError
from riak.transports.transport import RiakTransport
from riak.riak_object import VClock
from riak.ts_object import TsObject
from riak.util import decode_index_value, str_to_bytes, bytes_to_str
from riak.transports.pbc.connection import RiakPbcConnection
from riak.transports.pbc.stream import (RiakPbcKeyStream,
                                        RiakPbcMapredStream,
                                        RiakPbcBucketStream,
                                        RiakPbcIndexStream,
                                        RiakPbcTsKeyStream)
from riak.transports.pbc.codec import RiakPbcCodec
from six import PY2, PY3


class RiakPbcTransport(RiakTransport, RiakPbcConnection, RiakPbcCodec):
    """
    The RiakPbcTransport object holds a connection to the protocol
    buffers interface on the riak server.
    """

    def __init__(self,
                 node=None,
                 client=None,
                 timeout=None,
                 *unused_options):
        """
        Construct a new RiakPbcTransport object.
        """
        super(RiakPbcTransport, self).__init__()

        self._client = client
        self._node = node
        self._address = (node.host, node.pb_port)
        self._timeout = timeout
        self._socket = None

    # FeatureDetection API
    def _server_version(self):
        return bytes_to_str(self.get_server_info()['server_version'])

    def ping(self):
        """
        Ping the remote server
        """

        msg_code, msg = self._request(riak.pb.messages.MSG_CODE_PING_REQ)
        if msg_code == riak.pb.messages.MSG_CODE_PING_RESP:
            return True
        else:
            return False

    def get_server_info(self):
        """
        Get information about the server
        """
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_GET_SERVER_INFO_REQ,
            expect=riak.pb.messages.MSG_CODE_GET_SERVER_INFO_RESP)
        return {'node': bytes_to_str(resp.node),
                'server_version': bytes_to_str(resp.server_version)}

    def _get_client_id(self):
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_GET_CLIENT_ID_REQ,
            expect=riak.pb.messages.MSG_CODE_GET_CLIENT_ID_RESP)
        return bytes_to_str(resp.client_id)

    def _set_client_id(self, client_id):
        req = riak.pb.riak_kv_pb2.RpbSetClientIdReq()
        req.client_id = str_to_bytes(client_id)

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_SET_CLIENT_ID_REQ, req,
            riak.pb.messages.MSG_CODE_SET_CLIENT_ID_RESP)

        self._client_id = client_id

    client_id = property(_get_client_id, _set_client_id,
                         doc="""the client ID for this connection""")

    def get(self, robj, r=None, pr=None, timeout=None, basic_quorum=None,
            notfound_ok=None):
        """
        Serialize get request and deserialize response
        """
        bucket = robj.bucket

        req = riak.pb.riak_kv_pb2.RpbGetReq()
        if r:
            req.r = self._encode_quorum(r)
        if self.quorum_controls():
            if pr:
                req.pr = self._encode_quorum(pr)
            if basic_quorum is not None:
                req.basic_quorum = basic_quorum
            if notfound_ok is not None:
                req.notfound_ok = notfound_ok
        if self.client_timeouts() and timeout:
            req.timeout = timeout
        if self.tombstone_vclocks():
            req.deletedvclock = True

        req.bucket = str_to_bytes(bucket.name)
        self._add_bucket_type(req, bucket.bucket_type)

        req.key = str_to_bytes(robj.key)

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_GET_REQ, req,
            riak.pb.messages.MSG_CODE_GET_RESP)

        if resp is not None:
            if resp.HasField('vclock'):
                robj.vclock = VClock(resp.vclock, 'binary')
            # We should do this even if there are no contents, i.e.
            # the object is tombstoned
            self._decode_contents(resp.content, robj)
        else:
            # "not found" returns an empty message,
            # so let's make sure to clear the siblings
            robj.siblings = []

        return robj

    def put(self, robj, w=None, dw=None, pw=None, return_body=True,
            if_none_match=False, timeout=None):
        bucket = robj.bucket

        req = riak.pb.riak_kv_pb2.RpbPutReq()
        if w:
            req.w = self._encode_quorum(w)
        if dw:
            req.dw = self._encode_quorum(dw)
        if self.quorum_controls() and pw:
            req.pw = self._encode_quorum(pw)

        if return_body:
            req.return_body = 1
        if if_none_match:
            req.if_none_match = 1
        if self.client_timeouts() and timeout:
            req.timeout = timeout

        req.bucket = str_to_bytes(bucket.name)
        self._add_bucket_type(req, bucket.bucket_type)

        if robj.key:
            req.key = str_to_bytes(robj.key)
        if robj.vclock:
            req.vclock = robj.vclock.encode('binary')

        self._encode_content(robj, req.content)

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_PUT_REQ, req,
            riak.pb.messages.MSG_CODE_PUT_RESP)

        if resp is not None:
            if resp.HasField('key'):
                robj.key = bytes_to_str(resp.key)
            if resp.HasField("vclock"):
                robj.vclock = VClock(resp.vclock, 'binary')
            if resp.content:
                self._decode_contents(resp.content, robj)
        elif not robj.key:
            raise RiakError("missing response object")

        return robj

    def ts_get(self, table, key):
        req = riak.pb.riak_ts_pb2.TsGetReq()
        self._encode_timeseries_keyreq(table, key, req)

        msg_code, ts_get_resp = self._request(
            riak.pb.messages.MSG_CODE_TS_GET_REQ, req,
            riak.pb.messages.MSG_CODE_TS_GET_RESP)

        tsobj = TsObject(self._client, table, [], None)
        self._decode_timeseries(ts_get_resp, tsobj)
        return tsobj

    def ts_put(self, tsobj):
        req = riak.pb.riak_ts_pb2.TsPutReq()
        self._encode_timeseries_put(tsobj, req)

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_TS_PUT_REQ, req,
            riak.pb.messages.MSG_CODE_TS_PUT_RESP)

        if resp is not None:
            return True
        else:
            raise RiakError("missing response object")

    def ts_delete(self, table, key):
        req = riak.pb.riak_ts_pb2.TsDelReq()
        self._encode_timeseries_keyreq(table, key, req)

        msg_code, ts_del_resp = self._request(
            riak.pb.messages.MSG_CODE_TS_DEL_REQ, req,
            riak.pb.messages.MSG_CODE_TS_DEL_RESP)

        if ts_del_resp is not None:
            return True
        else:
            raise RiakError("missing response object")

    def ts_query(self, table, query, interpolations=None):
        req = riak.pb.riak_ts_pb2.TsQueryReq()
        req.query.base = str_to_bytes(query)

        msg_code, ts_query_resp = self._request(
            riak.pb.messages.MSG_CODE_TS_QUERY_REQ, req,
            riak.pb.messages.MSG_CODE_TS_QUERY_RESP)

        tsobj = TsObject(self._client, table, [], [])
        self._decode_timeseries(ts_query_resp, tsobj)
        return tsobj

    def ts_stream_keys(self, table, timeout=None):
        """
        Streams keys from a timeseries table, returning an iterator that
        yields lists of keys.
        """
        req = riak.pb.riak_ts_pb2.TsListKeysReq()
        t = None
        if self.client_timeouts() and timeout:
            t = timeout
        self._encode_timeseries_listkeysreq(table, req, t)

        self._send_msg(riak.pb.messages.MSG_CODE_TS_LIST_KEYS_REQ, req)

        return RiakPbcTsKeyStream(self)

    def delete(self, robj, rw=None, r=None, w=None, dw=None, pr=None, pw=None,
               timeout=None):
        req = riak.pb.riak_kv_pb2.RpbDelReq()
        if rw:
            req.rw = self._encode_quorum(rw)
        if r:
            req.r = self._encode_quorum(r)
        if w:
            req.w = self._encode_quorum(w)
        if dw:
            req.dw = self._encode_quorum(dw)

        if self.quorum_controls():
            if pr:
                req.pr = self._encode_quorum(pr)
            if pw:
                req.pw = self._encode_quorum(pw)

        if self.client_timeouts() and timeout:
            req.timeout = timeout

        use_vclocks = (self.tombstone_vclocks() and
                       hasattr(robj, 'vclock') and robj.vclock)
        if use_vclocks:
            req.vclock = robj.vclock.encode('binary')

        bucket = robj.bucket
        req.bucket = str_to_bytes(bucket.name)
        self._add_bucket_type(req, bucket.bucket_type)
        req.key = str_to_bytes(robj.key)

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_DEL_REQ, req,
            riak.pb.messages.MSG_CODE_DEL_RESP)
        return self

    def get_keys(self, bucket, timeout=None):
        """
        Lists all keys within a bucket.
        """
        keys = []
        for keylist in self.stream_keys(bucket, timeout=timeout):
            for key in keylist:
                keys.append(bytes_to_str(key))

        return keys

    def stream_keys(self, bucket, timeout=None):
        """
        Streams keys from a bucket, returning an iterator that yields
        lists of keys.
        """
        req = riak.pb.riak_kv_pb2.RpbListKeysReq()
        req.bucket = str_to_bytes(bucket.name)
        self._add_bucket_type(req, bucket.bucket_type)
        if self.client_timeouts() and timeout:
            req.timeout = timeout

        self._send_msg(riak.pb.messages.MSG_CODE_LIST_KEYS_REQ, req)

        return RiakPbcKeyStream(self)

    def get_buckets(self, bucket_type=None, timeout=None):
        """
        Serialize bucket listing request and deserialize response
        """
        req = riak.pb.riak_kv_pb2.RpbListBucketsReq()
        self._add_bucket_type(req, bucket_type)

        if self.client_timeouts() and timeout:
            req.timeout = timeout

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_LIST_BUCKETS_REQ, req,
            riak.pb.messages.MSG_CODE_LIST_BUCKETS_RESP)
        return resp.buckets

    def stream_buckets(self, bucket_type=None, timeout=None):
        """
        Stream list of buckets through an iterator
        """

        if not self.bucket_stream():
            raise NotImplementedError('Streaming list-buckets is not '
                                      'supported')

        req = riak.pb.riak_kv_pb2.RpbListBucketsReq()
        req.stream = True
        self._add_bucket_type(req, bucket_type)
        # Bucket streaming landed in the same release as timeouts, so
        # we don't need to check the capability.
        if timeout:
            req.timeout = timeout

        self._send_msg(riak.pb.messages.MSG_CODE_LIST_BUCKETS_REQ, req)

        return RiakPbcBucketStream(self)

    def get_bucket_props(self, bucket):
        """
        Serialize bucket property request and deserialize response
        """
        req = riak.pb.riak_pb2.RpbGetBucketReq()
        req.bucket = str_to_bytes(bucket.name)
        self._add_bucket_type(req, bucket.bucket_type)

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_GET_BUCKET_REQ, req,
            riak.pb.messages.MSG_CODE_GET_BUCKET_RESP)

        return self._decode_bucket_props(resp.props)

    def set_bucket_props(self, bucket, props):
        """
        Serialize set bucket property request and deserialize response
        """
        req = riak.pb.riak_pb2.RpbSetBucketReq()
        req.bucket = str_to_bytes(bucket.name)
        self._add_bucket_type(req, bucket.bucket_type)

        if not self.pb_all_bucket_props():
            for key in props:
                if key not in ('n_val', 'allow_mult'):
                    raise NotImplementedError('Server only supports n_val and '
                                              'allow_mult properties over PBC')

        self._encode_bucket_props(props, req)

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_SET_BUCKET_REQ, req,
            riak.pb.messages.MSG_CODE_SET_BUCKET_RESP)
        return True

    def clear_bucket_props(self, bucket):
        """
        Clear bucket properties, resetting them to their defaults
        """
        if not self.pb_clear_bucket_props():
            return False

        req = riak.pb.riak_pb2.RpbResetBucketReq()
        req.bucket = str_to_bytes(bucket.name)
        self._add_bucket_type(req, bucket.bucket_type)
        self._request(
            riak.pb.messages.MSG_CODE_RESET_BUCKET_REQ, req,
            riak.pb.messages.MSG_CODE_RESET_BUCKET_RESP)
        return True

    def get_bucket_type_props(self, bucket_type):
        """
        Fetch bucket-type properties
        """
        self._check_bucket_types(bucket_type)

        req = riak.pb.riak_pb2.RpbGetBucketTypeReq()
        req.type = str_to_bytes(bucket_type.name)

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_GET_BUCKET_TYPE_REQ, req,
            riak.pb.messages.MSG_CODE_GET_BUCKET_RESP)

        return self._decode_bucket_props(resp.props)

    def set_bucket_type_props(self, bucket_type, props):
        """
        Set bucket-type properties
        """
        self._check_bucket_types(bucket_type)

        req = riak.pb.riak_pb2.RpbSetBucketTypeReq()
        req.type = str_to_bytes(bucket_type.name)

        self._encode_bucket_props(props, req)

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_SET_BUCKET_TYPE_REQ, req,
            riak.pb.messages.MSG_CODE_SET_BUCKET_RESP)

        return True

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

        req = riak.pb.riak_kv_pb2.RpbMapRedReq()
        req.request = str_to_bytes(content)
        req.content_type = str_to_bytes("application/json")

        self._send_msg(riak.pb.messages.MSG_CODE_MAP_RED_REQ, req)

        return RiakPbcMapredStream(self)

    def get_index(self, bucket, index, startkey, endkey=None,
                  return_terms=None, max_results=None, continuation=None,
                  timeout=None, term_regex=None):
        if not self.pb_indexes():
            return self._get_index_mapred_emu(bucket, index, startkey, endkey)

        if term_regex and not self.index_term_regex():
            raise NotImplementedError("Secondary index term_regex is not "
                                      "supported")

        req = self._encode_index_req(bucket, index, startkey, endkey,
                                     return_terms, max_results, continuation,
                                     timeout, term_regex)

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_INDEX_REQ, req,
            riak.pb.messages.MSG_CODE_INDEX_RESP)

        if return_terms and resp.results:
            results = [(decode_index_value(index, pair.key),
                        bytes_to_str(pair.value))
                       for pair in resp.results]
        else:
            results = resp.keys[:]
            if PY3:
                results = [bytes_to_str(key) for key in resp.keys]

        if max_results is not None and resp.HasField('continuation'):
            return (results, bytes_to_str(resp.continuation))
        else:
            return (results, None)

    def stream_index(self, bucket, index, startkey, endkey=None,
                     return_terms=None, max_results=None, continuation=None,
                     timeout=None, term_regex=None):
        if not self.stream_indexes():
            raise NotImplementedError("Secondary index streaming is not "
                                      "supported")

        if term_regex and not self.index_term_regex():
            raise NotImplementedError("Secondary index term_regex is not "
                                      "supported")

        req = self._encode_index_req(bucket, index, startkey, endkey,
                                     return_terms, max_results, continuation,
                                     timeout, term_regex)
        req.stream = True

        self._send_msg(riak.pb.messages.MSG_CODE_INDEX_REQ, req)

        return RiakPbcIndexStream(self, index, return_terms)

    def create_search_index(self, index, schema=None, n_val=None,
                            timeout=None):
        if not self.pb_search_admin():
            raise NotImplementedError("Search 2.0 administration is not "
                                      "supported for this version")
        index = str_to_bytes(index)
        idx = riak.pb.riak_yokozuna_pb2.RpbYokozunaIndex(name=index)
        if schema:
            idx.schema = str_to_bytes(schema)
        if n_val:
            idx.n_val = n_val
        req = riak.pb.riak_yokozuna_pb2.RpbYokozunaIndexPutReq(index=idx)
        if timeout is not None:
            req.timeout = timeout

        self._request(
            riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_PUT_REQ, req,
            riak.pb.messages.MSG_CODE_PUT_RESP)

        return True

    def get_search_index(self, index):
        if not self.pb_search_admin():
            raise NotImplementedError("Search 2.0 administration is not "
                                      "supported for this version")
        req = riak.pb.riak_yokozuna_pb2.RpbYokozunaIndexGetReq(
                name=str_to_bytes(index))

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_GET_REQ, req,
            riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_GET_RESP)
        if len(resp.index) > 0:
            return self._decode_search_index(resp.index[0])
        else:
            raise RiakError('notfound')

    def list_search_indexes(self):
        if not self.pb_search_admin():
            raise NotImplementedError("Search 2.0 administration is not "
                                      "supported for this version")
        req = riak.pb.riak_yokozuna_pb2.RpbYokozunaIndexGetReq()

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_GET_REQ, req,
            riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_GET_RESP)

        return [self._decode_search_index(index) for index in resp.index]

    def delete_search_index(self, index):
        if not self.pb_search_admin():
            raise NotImplementedError("Search 2.0 administration is not "
                                      "supported for this version")
        req = riak.pb.riak_yokozuna_pb2.RpbYokozunaIndexDeleteReq(
                name=str_to_bytes(index))

        self._request(
            riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_DELETE_REQ, req,
            riak.pb.messages.MSG_CODE_DEL_RESP)

        return True

    def create_search_schema(self, schema, content):
        if not self.pb_search_admin():
            raise NotImplementedError("Search 2.0 administration is not "
                                      "supported for this version")
        scma = riak.pb.riak_yokozuna_pb2.RpbYokozunaSchema(
                name=str_to_bytes(schema),
                content=str_to_bytes(content))
        req = riak.pb.riak_yokozuna_pb2.RpbYokozunaSchemaPutReq(
                schema=scma)

        self._request(
            riak.pb.messages.MSG_CODE_YOKOZUNA_SCHEMA_PUT_REQ, req,
            riak.pb.messages.MSG_CODE_PUT_RESP)

        return True

    def get_search_schema(self, schema):
        if not self.pb_search_admin():
            raise NotImplementedError("Search 2.0 administration is not "
                                      "supported for this version")
        req = riak.pb.riak_yokozuna_pb2.RpbYokozunaSchemaGetReq(
                name=str_to_bytes(schema))

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_YOKOZUNA_SCHEMA_GET_REQ, req,
            riak.pb.messages.MSG_CODE_YOKOZUNA_SCHEMA_GET_RESP)

        result = {}
        result['name'] = bytes_to_str(resp.schema.name)
        result['content'] = bytes_to_str(resp.schema.content)
        return result

    def search(self, index, query, **params):
        if not self.pb_search():
            return self._search_mapred_emu(index, query)

        if PY2 and isinstance(query, unicode):  # noqa
            query = query.encode('utf8')

        req = riak.pb.riak_search_pb2.RpbSearchQueryReq(
                index=str_to_bytes(index),
                q=str_to_bytes(query))
        self._encode_search_query(req, params)

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_SEARCH_QUERY_REQ, req,
            riak.pb.messages.MSG_CODE_SEARCH_QUERY_RESP)

        result = {}
        if resp.HasField('max_score'):
            result['max_score'] = resp.max_score
        if resp.HasField('num_found'):
            result['num_found'] = resp.num_found
        result['docs'] = [self._decode_search_doc(doc) for doc in resp.docs]
        return result

    def get_counter(self, bucket, key, **params):
        if not bucket.bucket_type.is_default():
            raise NotImplementedError("Counters are not "
                                      "supported with bucket-types, "
                                      "use datatypes instead.")

        if not self.counters():
            raise NotImplementedError("Counters are not supported")

        req = riak.pb.riak_kv_pb2.RpbCounterGetReq()
        req.bucket = str_to_bytes(bucket.name)
        req.key = str_to_bytes(key)
        if params.get('r') is not None:
            req.r = self._encode_quorum(params['r'])
        if params.get('pr') is not None:
            req.pr = self._encode_quorum(params['pr'])
        if params.get('basic_quorum') is not None:
            req.basic_quorum = params['basic_quorum']
        if params.get('notfound_ok') is not None:
            req.notfound_ok = params['notfound_ok']

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_COUNTER_GET_REQ, req,
            riak.pb.messages.MSG_CODE_COUNTER_GET_RESP)
        if resp.HasField('value'):
            return resp.value
        else:
            return None

    def update_counter(self, bucket, key, value, **params):
        if not bucket.bucket_type.is_default():
            raise NotImplementedError("Counters are not "
                                      "supported with bucket-types, "
                                      "use datatypes instead.")

        if not self.counters():
            raise NotImplementedError("Counters are not supported")

        req = riak.pb.riak_kv_pb2.RpbCounterUpdateReq()
        req.bucket = str_to_bytes(bucket.name)
        req.key = str_to_bytes(key)
        req.amount = value
        if params.get('w') is not None:
            req.w = self._encode_quorum(params['w'])
        if params.get('dw') is not None:
            req.dw = self._encode_quorum(params['dw'])
        if params.get('pw') is not None:
            req.pw = self._encode_quorum(params['pw'])
        if params.get('returnvalue') is not None:
            req.returnvalue = params['returnvalue']

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_COUNTER_UPDATE_REQ, req,
            riak.pb.messages.MSG_CODE_COUNTER_UPDATE_RESP)
        if resp.HasField('value'):
            return resp.value
        else:
            return True

    def fetch_datatype(self, bucket, key, **options):

        if bucket.bucket_type.is_default():
            raise NotImplementedError("Datatypes cannot be used in the default"
                                      " bucket-type.")

        if not self.datatypes():
            raise NotImplementedError("Datatypes are not supported.")

        req = riak.pb.riak_dt_pb2.DtFetchReq()
        req.type = str_to_bytes(bucket.bucket_type.name)
        req.bucket = str_to_bytes(bucket.name)
        req.key = str_to_bytes(key)
        self._encode_dt_options(req, options)

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_DT_FETCH_REQ, req,
            riak.pb.messages.MSG_CODE_DT_FETCH_RESP)

        return self._decode_dt_fetch(resp)

    def update_datatype(self, datatype, **options):

        if datatype.bucket.bucket_type.is_default():
            raise NotImplementedError("Datatypes cannot be used in the default"
                                      " bucket-type.")

        if not self.datatypes():
            raise NotImplementedError("Datatypes are not supported.")

        op = datatype.to_op()
        type_name = datatype.type_name
        if not op:
            raise ValueError("No operation to send on datatype {!r}".
                             format(datatype))

        req = riak.pb.riak_dt_pb2.DtUpdateReq()
        req.bucket = str_to_bytes(datatype.bucket.name)
        req.type = str_to_bytes(datatype.bucket.bucket_type.name)

        if datatype.key:
            req.key = str_to_bytes(datatype.key)
        if datatype._context:
            req.context = datatype._context

        self._encode_dt_options(req, options)

        self._encode_dt_op(type_name, req, op)

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_DT_UPDATE_REQ, req,
            riak.pb.messages.MSG_CODE_DT_UPDATE_RESP)
        if resp.HasField('key'):
            datatype.key = resp.key[:]
        if resp.HasField('context'):
            datatype._context = resp.context[:]

        if options.get('return_body'):
            datatype._set_value(self._decode_dt_value(type_name, resp))

        return True

    def get_preflist(self, bucket, key):
        """
        Get the preflist for a bucket/key

        :param bucket: Riak Bucket
        :type bucket: :class:`~riak.bucket.RiakBucket`
        :param key: Riak Key
        :type key: string
        :rtype: list of dicts
        """
        req = riak.pb.riak_kv_pb2.RpbGetBucketKeyPreflistReq()
        req.bucket = str_to_bytes(bucket.name)
        req.key = str_to_bytes(key)
        req.type = str_to_bytes(bucket.bucket_type.name)

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_GET_BUCKET_KEY_PREFLIST_REQ, req,
            riak.pb.messages.MSG_CODE_GET_BUCKET_KEY_PREFLIST_RESP)

        return [self._decode_preflist(item) for item in resp.preflist]
