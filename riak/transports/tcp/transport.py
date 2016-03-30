# TODO RTS-842 codecs should return msg codes too
import erlastic
import six
import riak.pb.messages

from riak import RiakError
from riak.codecs.pbuf import PbufCodec
from riak.codecs.ttb import TtbCodec
from riak.transports.transport import Transport
from riak.ts_object import TsObject

# TODO RTS-842 ideally these would not be needed
from riak.util import decode_index_value, bytes_to_str

from riak.transports.tcp.connection import TcpConnection
from riak.transports.tcp.stream import (PbufKeyStream,
                                        PbufMapredStream,
                                        PbufBucketStream,
                                        PbufIndexStream,
                                        PbufTsKeyStream)


class TcpTransport(Transport, TcpConnection):
    """
    The TcpTransport object holds a connection to the TCP
    socket on the Riak server.
    """
    def __init__(self,
                 node=None,
                 client=None,
                 timeout=None,
                 **kwargs):
        super(TcpTransport, self).__init__()

        self._client = client
        self._node = node
        self._address = (node.host, node.pb_port)
        self._timeout = timeout
        self._socket = None
        self._pbuf_c = None
        self._ttb_c = None
        self._use_ttb = kwargs.get('use_ttb', True)

    def _get_pbuf_codec(self):
        if not self._pbuf_c:
            self._pbuf_c = PbufCodec(
                    self.client_timeouts(), self.quorum_controls(),
                    self.tombstone_vclocks(), self.bucket_types())
        return self._pbuf_c

    def _get_codec(self, ttb_supported=False):
        if ttb_supported:
            if self._use_ttb:
                if not self._enable_ttb():
                    raise RiakError('could not switch to TTB encoding!')
                if not self._ttb_c:
                    self._ttb_c = TtbCodec()
                codec = self._ttb_c
            else:
                codec = self._get_pbuf_codec()
        else:
            codec = self._get_pbuf_codec()
        return codec

    # FeatureDetection API
    def _server_version(self):
        server_info = self.get_server_info()
        return server_info['server_version']

    def ping(self):
        """
        Ping the remote server
        """
        resp_code, _ = self._request(
                riak.pb.messages.MSG_CODE_PING_REQ,
                None,
                riak.pb.messages.MSG_CODE_PING_RESP)
        if resp_code == riak.pb.messages.MSG_CODE_PING_RESP:
            return True
        else:
            return False

    def get_server_info(self):
        """
        Get information about the server
        """
        # NB: can't do it this way due to recursion
        # codec = self._get_codec(ttb_supported=False)
        codec = PbufCodec()
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_GET_SERVER_INFO_REQ,
            expect=riak.pb.messages.MSG_CODE_GET_SERVER_INFO_RESP)
        return codec._decode_get_server_info(resp)

    def _get_client_id(self):
        codec = self._get_codec(ttb_supported=False)
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_GET_CLIENT_ID_REQ,
            expect=riak.pb.messages.MSG_CODE_GET_CLIENT_ID_RESP)
        return codec._decode_get_client_id(resp)

    def _set_client_id(self, client_id):
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_set_client_id(client_id)
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_SET_CLIENT_ID_REQ, data,
            riak.pb.messages.MSG_CODE_SET_CLIENT_ID_RESP)
        self._client_id = client_id

    client_id = property(_get_client_id, _set_client_id,
                         doc="""the client ID for this connection""")

    def get(self, robj, r=None, pr=None, timeout=None, basic_quorum=None,
            notfound_ok=None):
        """
        Serialize get request and deserialize response
        """
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_get(robj, r, pr,
                                 timeout, basic_quorum, notfound_ok)
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_GET_REQ, data,
            riak.pb.messages.MSG_CODE_GET_RESP)
        return codec._decode_get(robj, resp)

    def put(self, robj, w=None, dw=None, pw=None, return_body=True,
            if_none_match=False, timeout=None):
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_put(robj, w, dw, pw, return_body,
                                 if_none_match, timeout)
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_PUT_REQ, data,
            riak.pb.messages.MSG_CODE_PUT_RESP)
        return codec._decode_put(robj, resp)

    def ts_describe(self, table):
        query = 'DESCRIBE {table}'.format(table=table.name)
        return self.ts_query(table, query)

    def ts_get(self, table, key):
        codec = self._get_codec(ttb_supported=True)
        data = codec._encode_timeseries_keyreq(table, key)
        msg_code, ts_get_resp = self._request(
            riak.pb.messages.MSG_CODE_TS_GET_REQ, data,
            riak.pb.messages.MSG_CODE_TS_GET_RESP)
        tsobj = TsObject(self._client, table, [], None)
        codec._decode_timeseries(ts_get_resp, tsobj)
        return tsobj

    def ts_put(self, tsobj):
        codec = self._get_codec(ttb_supported=True)
        # TODO RTS-842 codecs should return msg codes too
        data = codec._encode_timeseries_put(tsobj)
        # logging.debug("pbc/transport ts_put _use_ttb: '%s'",
        #    self._use_ttb)
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_TS_PUT_REQ, data,
            riak.pb.messages.MSG_CODE_TS_PUT_RESP,
            self._use_ttb)
        if self._use_ttb and \
                resp is None and \
                msg_code == riak.pb.messages.MSG_CODE_TS_PUT_RESP:
            return True
        if resp is not None:
            return True
        else:
            raise RiakError("missing response object")

    def ts_delete(self, table, key):
        codec = self._get_codec(ttb_supported=True)
        data = codec._encode_timeseries_keyreq(table, key, is_delete=True)
        msg_code, ts_del_resp = self._request(
            riak.pb.messages.MSG_CODE_TS_DEL_REQ, data,
            riak.pb.messages.MSG_CODE_TS_DEL_RESP)
        if ts_del_resp is not None:
            return True
        else:
            raise RiakError("missing response object")

    def ts_query(self, table, query, interpolations=None):
        codec = self._get_codec(ttb_supported=True)
        data = codec._encode_timeseries_query(table, query, interpolations)
        msg_code, ts_query_resp = self._request(
            riak.pb.messages.MSG_CODE_TS_QUERY_REQ, data,
            riak.pb.messages.MSG_CODE_TS_QUERY_RESP)
        tsobj = TsObject(self._client, table, [], [])
        self._decode_timeseries(ts_query_resp, tsobj)
        return tsobj

    def ts_stream_keys(self, table, timeout=None):
        """
        Streams keys from a timeseries table, returning an iterator that
        yields lists of keys.
        """
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_timeseries_listkeysreq(table, timeout)
        self._send_msg(riak.pb.messages.MSG_CODE_TS_LIST_KEYS_REQ, data)
        return PbufTsKeyStream(self)

    def delete(self, robj, rw=None, r=None, w=None, dw=None, pr=None, pw=None,
               timeout=None):
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_delete(robj, rw, r, w, dw, pr, pw, timeout)
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_DEL_REQ, data,
            riak.pb.messages.MSG_CODE_DEL_RESP)
        return self

    def get_keys(self, bucket, timeout=None):
        """
        Lists all keys within a bucket.
        """
        codec = self._get_codec(ttb_supported=False)
        stream = self.stream_keys(bucket, timeout=timeout)
        return codec._decode_get_keys(stream)

    def stream_keys(self, bucket, timeout=None):
        """
        Streams keys from a bucket, returning an iterator that yields
        lists of keys.
        """
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_stream_keys(bucket, timeout)
        self._send_msg(riak.pb.messages.MSG_CODE_LIST_KEYS_REQ, data)
        return PbufKeyStream(self)

    def get_buckets(self, bucket_type=None, timeout=None):
        """
        Serialize bucket listing request and deserialize response
        """
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_get_buckets(bucket_type, timeout)
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_LIST_BUCKETS_REQ, data,
            riak.pb.messages.MSG_CODE_LIST_BUCKETS_RESP)
        return resp.buckets

    def stream_buckets(self, bucket_type=None, timeout=None):
        """
        Stream list of buckets through an iterator
        """
        if not self.bucket_stream():
            raise NotImplementedError('Streaming list-buckets is not '
                                      'supported')
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_stream_buckets(bucket_type, timeout)
        self._send_msg(riak.pb.messages.MSG_CODE_LIST_BUCKETS_REQ, data)
        return PbufBucketStream(self)

    def get_bucket_props(self, bucket):
        """
        Serialize bucket property request and deserialize response
        """
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_get_bucket_props(bucket)
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_GET_BUCKET_REQ, data,
            riak.pb.messages.MSG_CODE_GET_BUCKET_RESP)
        return codec._decode_bucket_props(resp.props)

    def set_bucket_props(self, bucket, props):
        """
        Serialize set bucket property request and deserialize response
        """
        if not self.pb_all_bucket_props():
            for key in props:
                if key not in ('n_val', 'allow_mult'):
                    raise NotImplementedError('Server only supports n_val and '
                                              'allow_mult properties over PBC')
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_set_bucket_props(bucket, props)
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_SET_BUCKET_REQ, data,
            riak.pb.messages.MSG_CODE_SET_BUCKET_RESP)
        return True

    def clear_bucket_props(self, bucket):
        """
        Clear bucket properties, resetting them to their defaults
        """
        if not self.pb_clear_bucket_props():
            return False
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_clear_bucket_props(bucket)
        self._request(
            riak.pb.messages.MSG_CODE_RESET_BUCKET_REQ, data,
            riak.pb.messages.MSG_CODE_RESET_BUCKET_RESP)
        return True

    def get_bucket_type_props(self, bucket_type):
        """
        Fetch bucket-type properties
        """
        self._check_bucket_types(bucket_type)
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_get_bucket_type_props(bucket_type)
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_GET_BUCKET_TYPE_REQ, data,
            riak.pb.messages.MSG_CODE_GET_BUCKET_RESP)
        return codec._decode_bucket_props(resp.props)

    def set_bucket_type_props(self, bucket_type, props):
        """
        Set bucket-type properties
        """
        self._check_bucket_types(bucket_type)
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_set_bucket_type_props(bucket_type, props)
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_SET_BUCKET_TYPE_REQ, data,
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
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_stream_mapred(content)
        self._send_msg(riak.pb.messages.MSG_CODE_MAP_RED_REQ, data)
        return PbufMapredStream(self)

    def get_index(self, bucket, index, startkey, endkey=None,
                  return_terms=None, max_results=None, continuation=None,
                  timeout=None, term_regex=None):
        # TODO RTS-842 NUKE THIS
        if not self.pb_indexes():
            return self._get_index_mapred_emu(bucket, index, startkey, endkey)

        if term_regex and not self.index_term_regex():
            raise NotImplementedError("Secondary index term_regex is not "
                                      "supported")

        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_index_req(bucket, index, startkey, endkey,
                                       return_terms, max_results,
                                       continuation, timeout, term_regex,
                                       streaming=False)

        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_INDEX_REQ, data,
            riak.pb.messages.MSG_CODE_INDEX_RESP)

        if return_terms and resp.results:
            results = [(decode_index_value(index, pair.key),
                        bytes_to_str(pair.value))
                       for pair in resp.results]
        else:
            results = resp.keys[:]
            if six.PY3:
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
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_index_req(bucket, index, startkey, endkey,
                                       return_terms, max_results,
                                       continuation, timeout,
                                       term_regex, streaming=True)
        self._send_msg(riak.pb.messages.MSG_CODE_INDEX_REQ, data)
        return PbufIndexStream(self, index, return_terms)

    def create_search_index(self, index, schema=None, n_val=None,
                            timeout=None):
        if not self.pb_search_admin():
            raise NotImplementedError("Search 2.0 administration is not "
                                      "supported for this version")
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_create_search_index(index, schema, n_val, timeout)
        self._request(
            riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_PUT_REQ, data,
            riak.pb.messages.MSG_CODE_PUT_RESP)
        return True

    def get_search_index(self, index):
        if not self.pb_search_admin():
            raise NotImplementedError("Search 2.0 administration is not "
                                      "supported for this version")
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_get_search_index(index)
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_GET_REQ, data,
            riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_GET_RESP)
        if len(resp.index) > 0:
            return codec._decode_search_index(resp.index[0])
        else:
            raise RiakError('notfound')

    def list_search_indexes(self):
        if not self.pb_search_admin():
            raise NotImplementedError("Search 2.0 administration is not "
                                      "supported for this version")
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_list_search_indexes()
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_GET_REQ, data,
            riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_GET_RESP)
        return [codec._decode_search_index(index) for index in resp.index]

    def delete_search_index(self, index):
        if not self.pb_search_admin():
            raise NotImplementedError("Search 2.0 administration is not "
                                      "supported for this version")
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_delete_search_index(index)
        self._request(
            riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_DELETE_REQ, data,
            riak.pb.messages.MSG_CODE_DEL_RESP)
        return True

    def create_search_schema(self, schema, content):
        if not self.pb_search_admin():
            raise NotImplementedError("Search 2.0 administration is not "
                                      "supported for this version")
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_create_search_schema(schema, content)
        self._request(
            riak.pb.messages.MSG_CODE_YOKOZUNA_SCHEMA_PUT_REQ, data,
            riak.pb.messages.MSG_CODE_PUT_RESP)
        return True

    def get_search_schema(self, schema):
        if not self.pb_search_admin():
            raise NotImplementedError("Search 2.0 administration is not "
                                      "supported for this version")
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_get_search_schema(schema)
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_YOKOZUNA_SCHEMA_GET_REQ, data,
            riak.pb.messages.MSG_CODE_YOKOZUNA_SCHEMA_GET_RESP)
        return codec._decode_get_search_schema(resp)

    def search(self, index, query, **kwargs):
        # TODO RTS-842 NUKE THIS
        if not self.pb_search():
            return self._search_mapred_emu(index, query)
        # TODO RTS-842 six.u() instead?
        if six.PY2 and isinstance(query, unicode):  # noqa
            query = query.encode('utf8')
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_search(index, query, **kwargs)
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_SEARCH_QUERY_REQ, data,
            riak.pb.messages.MSG_CODE_SEARCH_QUERY_RESP)
        return codec._decode_search(resp)

    def get_counter(self, bucket, key, **kwargs):
        if not bucket.bucket_type.is_default():
            raise NotImplementedError("Counters are not "
                                      "supported with bucket-types, "
                                      "use datatypes instead.")
        if not self.counters():
            raise NotImplementedError("Counters are not supported")
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_get_counter(bucket, key, **kwargs)
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_COUNTER_GET_REQ, data,
            riak.pb.messages.MSG_CODE_COUNTER_GET_RESP)
        if resp.HasField('value'):
            return resp.value
        else:
            return None

    def update_counter(self, bucket, key, value, **kwargs):
        if not bucket.bucket_type.is_default():
            raise NotImplementedError("Counters are not "
                                      "supported with bucket-types, "
                                      "use datatypes instead.")
        if not self.counters():
            raise NotImplementedError("Counters are not supported")
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_update_counter(bucket, key, value, **kwargs)
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_COUNTER_UPDATE_REQ, data,
            riak.pb.messages.MSG_CODE_COUNTER_UPDATE_RESP)
        if resp.HasField('value'):
            return resp.value
        else:
            return True

    def fetch_datatype(self, bucket, key, **kwargs):
        if bucket.bucket_type.is_default():
            raise NotImplementedError("Datatypes cannot be used in the default"
                                      " bucket-type.")
        if not self.datatypes():
            raise NotImplementedError("Datatypes are not supported.")
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_fetch_datatype(bucket, key, **kwargs)
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_DT_FETCH_REQ, data,
            riak.pb.messages.MSG_CODE_DT_FETCH_RESP)
        return codec._decode_dt_fetch(resp)

    def update_datatype(self, datatype, **kwargs):
        if datatype.bucket.bucket_type.is_default():
            raise NotImplementedError("Datatypes cannot be used in the default"
                                      " bucket-type.")
        if not self.datatypes():
            raise NotImplementedError("Datatypes are not supported.")
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_update_datatype(datatype, **kwargs)
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_DT_UPDATE_REQ, data,
            riak.pb.messages.MSG_CODE_DT_UPDATE_RESP)
        codec._decode_update_datatype(datatype, resp, **kwargs)
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
        codec = self._get_codec(ttb_supported=False)
        data = codec._encode_get_preflist(bucket, key)
        msg_code, resp = self._request(
            riak.pb.messages.MSG_CODE_GET_BUCKET_KEY_PREFLIST_REQ, data,
            riak.pb.messages.MSG_CODE_GET_BUCKET_KEY_PREFLIST_RESP)
        return [codec._decode_preflist(item) for item in resp.preflist]

    # TODO RTS-842 is_ttb
    def _parse_msg(self, code, packet, is_ttb=False):
        if is_ttb:
            if code != riak.pb.messages.MSG_CODE_TS_GET_RESP and \
               code != riak.pb.messages.MSG_CODE_TS_PUT_RESP:
                raise RiakError("TTB can't parse code: %d" % code)
            if len(packet) > 0:
                return erlastic.decode(packet)
            else:
                return None
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

    def _maybe_riak_error(self, msg_code, data=None, is_ttb=False):
        if msg_code is riak.pb.messages.MSG_CODE_ERROR_RESP:
            if data is None:
                raise RiakError('no error provided!')
            # TODO RTS-842 TTB-specific version
            err = self._parse_msg(msg_code, data, is_ttb)
            if err is None:
                raise RiakError('no error provided!')
            else:
                raise RiakError(bytes_to_str(err.errmsg))

    def _maybe_incorrect_code(self, resp_code, expect=None):
        if expect and resp_code != expect:
            raise RiakError("unexpected message code: %d, expected %d"
                            % (resp_code, expect))

    # TODO RTS-842 is_ttb
    def _request(self, msg_code, data=None, expect=None, is_ttb=False):
        resp_code, data = self._send_recv(msg_code, data)
        self._maybe_riak_error(resp_code, data, is_ttb)
        self._maybe_incorrect_code(resp_code, expect)
        if resp_code in riak.pb.messages.MESSAGE_CLASSES:
            msg = self._parse_msg(resp_code, data, is_ttb)
        else:
            raise Exception("unknown msg code %s" % resp_code)
        # logging.debug("tcp/connection received resp_code %d msg %s",
        # resp_code, msg)
        return resp_code, msg
