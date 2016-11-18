import six

import riak.pb.messages

from riak import RiakError
from riak.codecs import Codec, Msg
from riak.codecs.pbuf import PbufCodec
from riak.codecs.ttb import TtbCodec
from riak.pb.messages import MSG_CODE_TS_TTB_MSG
from riak.transports.pool import BadResource
from riak.transports.transport import Transport
from riak.ts_object import TsObject

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
        self._socket_tcp_options = \
            kwargs.get('socket_tcp_options', {})
        self._socket_keepalive = \
            kwargs.get('socket_keepalive', False)
        self._ts_convert_timestamp = \
            kwargs.get('ts_convert_timestamp', False)
        self._use_ttb = \
            kwargs.get('use_ttb', True)

    def _get_pbuf_codec(self):
        if not self._pbuf_c:
            self._pbuf_c = PbufCodec(
                    self.client_timeouts(), self.quorum_controls(),
                    self.tombstone_vclocks(), self.bucket_types())
        return self._pbuf_c

    def _get_ttb_codec(self):
        if self._use_ttb:
            if not self._ttb_c:
                self._ttb_c = TtbCodec()
            codec = self._ttb_c
        else:
            codec = self._get_pbuf_codec()
        return codec

    def _get_codec(self, msg_code):
        if msg_code == MSG_CODE_TS_TTB_MSG:
            codec = self._get_ttb_codec()
        elif msg_code == riak.pb.messages.MSG_CODE_TS_GET_REQ:
            codec = self._get_ttb_codec()
        elif msg_code == riak.pb.messages.MSG_CODE_TS_PUT_REQ:
            codec = self._get_ttb_codec()
        elif msg_code == riak.pb.messages.MSG_CODE_TS_QUERY_REQ:
            codec = self._get_ttb_codec()
        else:
            codec = self._get_pbuf_codec()
        return codec

    # FeatureDetection API
    def _server_version(self):
        server_info = self.get_server_info()
        ver = server_info['server_version']
        (maj, min, patch) = [int(v) for v in ver.split('.')]
        if maj == 0:
            import datetime
            now = datetime.datetime.now()
            if now.year == 2016:
                # GH-471 As of 20160509 Riak TS OSS 1.3.0 returns '0.8.0' as
                # the version string.
                return '2.1.1'
        return ver

    def ping(self):
        """
        Ping the remote server
        """
        msg_code = riak.pb.messages.MSG_CODE_PING_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_ping()
        resp_code, _ = self._request(msg, codec)
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
        msg = Msg(riak.pb.messages.MSG_CODE_GET_SERVER_INFO_REQ, None,
                  riak.pb.messages.MSG_CODE_GET_SERVER_INFO_RESP)
        resp_code, resp = self._request(msg, codec)
        return codec.decode_get_server_info(resp)

    def _get_client_id(self):
        msg_code = riak.pb.messages.MSG_CODE_GET_CLIENT_ID_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_get_client_id()
        resp_code, resp = self._request(msg, codec)
        return codec.decode_get_client_id(resp)

    def _set_client_id(self, client_id):
        msg_code = riak.pb.messages.MSG_CODE_SET_CLIENT_ID_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_set_client_id(client_id)
        resp_code, resp = self._request(msg, codec)
        self._client_id = client_id

    client_id = property(_get_client_id, _set_client_id,
                         doc="""the client ID for this connection""")

    def get(self, robj, r=None, pr=None, timeout=None, basic_quorum=None,
            notfound_ok=None, head_only=False):
        """
        Serialize get request and deserialize response
        """
        msg_code = riak.pb.messages.MSG_CODE_GET_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_get(robj, r, pr,
                               timeout, basic_quorum,
                               notfound_ok, head_only)
        resp_code, resp = self._request(msg, codec)
        return codec.decode_get(robj, resp)

    def put(self, robj, w=None, dw=None, pw=None, return_body=True,
            if_none_match=False, timeout=None):
        msg_code = riak.pb.messages.MSG_CODE_PUT_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_put(robj, w, dw, pw, return_body,
                               if_none_match, timeout)
        resp_code, resp = self._request(msg, codec)
        return codec.decode_put(robj, resp)

    def ts_describe(self, table):
        query = 'DESCRIBE {table}'.format(table=table.name)
        return self.ts_query(table, query)

    def ts_get(self, table, key):
        msg_code = MSG_CODE_TS_TTB_MSG
        codec = self._get_codec(msg_code)
        msg = codec.encode_timeseries_keyreq(table, key)
        resp_code, resp = self._request(msg, codec)
        tsobj = TsObject(self._client, table)
        codec.decode_timeseries(resp, tsobj,
                                self._ts_convert_timestamp)
        return tsobj

    def ts_put(self, tsobj):
        msg_code = MSG_CODE_TS_TTB_MSG
        codec = self._get_codec(msg_code)
        msg = codec.encode_timeseries_put(tsobj)
        resp_code, resp = self._request(msg, codec)
        return codec.validate_timeseries_put_resp(resp_code, resp)

    def ts_delete(self, table, key):
        msg_code = riak.pb.messages.MSG_CODE_TS_DEL_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_timeseries_keyreq(table, key, is_delete=True)
        resp_code, resp = self._request(msg, codec)
        if resp is not None:
            return True
        else:
            raise RiakError("missing response object")

    def ts_query(self, table, query, interpolations=None):
        msg_code = riak.pb.messages.MSG_CODE_TS_QUERY_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_timeseries_query(table, query, interpolations)
        resp_code, resp = self._request(msg, codec)
        tsobj = TsObject(self._client, table)
        codec.decode_timeseries(resp, tsobj,
                                self._ts_convert_timestamp)
        return tsobj

    def ts_stream_keys(self, table, timeout=None):
        """
        Streams keys from a timeseries table, returning an iterator that
        yields lists of keys.
        """
        msg_code = riak.pb.messages.MSG_CODE_TS_LIST_KEYS_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_timeseries_listkeysreq(table, timeout)
        self._send_msg(msg.msg_code, msg.data)
        return PbufTsKeyStream(self, codec, self._ts_convert_timestamp)

    def delete(self, robj, rw=None, r=None, w=None, dw=None,
               pr=None, pw=None, timeout=None):
        msg_code = riak.pb.messages.MSG_CODE_DEL_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_delete(robj, rw, r, w, dw, pr, pw, timeout)
        resp_code, resp = self._request(msg, codec)
        return self

    def get_keys(self, bucket, timeout=None):
        """
        Lists all keys within a bucket.
        """
        msg_code = riak.pb.messages.MSG_CODE_LIST_KEYS_REQ
        codec = self._get_codec(msg_code)
        stream = self.stream_keys(bucket, timeout=timeout)
        return codec.decode_get_keys(stream)

    def stream_keys(self, bucket, timeout=None):
        """
        Streams keys from a bucket, returning an iterator that yields
        lists of keys.
        """
        msg_code = riak.pb.messages.MSG_CODE_LIST_KEYS_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_stream_keys(bucket, timeout)
        self._send_msg(msg.msg_code, msg.data)
        return PbufKeyStream(self, codec)

    def get_buckets(self, bucket_type=None, timeout=None):
        """
        Serialize bucket listing request and deserialize response
        """
        msg_code = riak.pb.messages.MSG_CODE_LIST_BUCKETS_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_get_buckets(bucket_type,
                                       timeout, streaming=False)
        resp_code, resp = self._request(msg, codec)
        return resp.buckets

    def stream_buckets(self, bucket_type=None, timeout=None):
        """
        Stream list of buckets through an iterator
        """
        if not self.bucket_stream():
            raise NotImplementedError('Streaming list-buckets is not '
                                      'supported')
        msg_code = riak.pb.messages.MSG_CODE_LIST_BUCKETS_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_get_buckets(bucket_type,
                                       timeout, streaming=True)
        self._send_msg(msg.msg_code, msg.data)
        return PbufBucketStream(self, codec)

    def get_bucket_props(self, bucket):
        """
        Serialize bucket property request and deserialize response
        """
        msg_code = riak.pb.messages.MSG_CODE_GET_BUCKET_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_get_bucket_props(bucket)
        resp_code, resp = self._request(msg, codec)
        return codec.decode_bucket_props(resp.props)

    def set_bucket_props(self, bucket, props):
        """
        Serialize set bucket property request and deserialize response
        """
        if not self.pb_all_bucket_props():
            for key in props:
                if key not in ('n_val', 'allow_mult'):
                    raise NotImplementedError('Server only supports n_val and '
                                              'allow_mult properties over PBC')
        msg_code = riak.pb.messages.MSG_CODE_SET_BUCKET_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_set_bucket_props(bucket, props)
        resp_code, resp = self._request(msg, codec)
        return True

    def clear_bucket_props(self, bucket):
        """
        Clear bucket properties, resetting them to their defaults
        """
        if not self.pb_clear_bucket_props():
            return False
        msg_code = riak.pb.messages.MSG_CODE_RESET_BUCKET_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_clear_bucket_props(bucket)
        self._request(msg, codec)
        return True

    def get_bucket_type_props(self, bucket_type):
        """
        Fetch bucket-type properties
        """
        self._check_bucket_types(bucket_type)
        msg_code = riak.pb.messages.MSG_CODE_GET_BUCKET_TYPE_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_get_bucket_type_props(bucket_type)
        resp_code, resp = self._request(msg, codec)
        return codec.decode_bucket_props(resp.props)

    def set_bucket_type_props(self, bucket_type, props):
        """
        Set bucket-type properties
        """
        self._check_bucket_types(bucket_type)
        msg_code = riak.pb.messages.MSG_CODE_SET_BUCKET_TYPE_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_set_bucket_type_props(bucket_type, props)
        resp_code, resp = self._request(msg, codec)
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
        msg_code = riak.pb.messages.MSG_CODE_MAP_RED_REQ
        codec = self._get_codec(msg_code)
        content = self._construct_mapred_json(inputs, query, timeout)
        msg = codec.encode_stream_mapred(content)
        self._send_msg(msg.msg_code, msg.data)
        return PbufMapredStream(self, codec)

    def get_index(self, bucket, index, startkey, endkey=None,
                  return_terms=None, max_results=None, continuation=None,
                  timeout=None, term_regex=None):
        # TODO FUTURE NUKE THIS MAPRED
        if not self.pb_indexes():
            return self._get_index_mapred_emu(bucket, index, startkey, endkey)

        if term_regex and not self.index_term_regex():
            raise NotImplementedError("Secondary index term_regex is not "
                                      "supported")

        msg_code = riak.pb.messages.MSG_CODE_INDEX_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_index_req(bucket, index, startkey, endkey,
                                     return_terms, max_results,
                                     continuation, timeout,
                                     term_regex, streaming=False)
        resp_code, resp = self._request(msg, codec)
        return codec.decode_index_req(resp, index,
                                      return_terms, max_results)

    def stream_index(self, bucket, index, startkey, endkey=None,
                     return_terms=None, max_results=None, continuation=None,
                     timeout=None, term_regex=None):
        if not self.stream_indexes():
            raise NotImplementedError("Secondary index streaming is not "
                                      "supported")
        if term_regex and not self.index_term_regex():
            raise NotImplementedError("Secondary index term_regex is not "
                                      "supported")
        msg_code = riak.pb.messages.MSG_CODE_INDEX_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_index_req(bucket, index, startkey, endkey,
                                     return_terms, max_results,
                                     continuation, timeout,
                                     term_regex, streaming=True)
        self._send_msg(msg.msg_code, msg.data)
        return PbufIndexStream(self, codec, index, return_terms)

    def create_search_index(self, index, schema=None, n_val=None,
                            timeout=None):
        if not self.pb_search_admin():
            raise NotImplementedError("Search 2.0 administration is not "
                                      "supported for this version")
        msg_code = riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_PUT_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_create_search_index(index, schema, n_val, timeout)
        self._request(msg, codec)
        return True

    def get_search_index(self, index):
        if not self.pb_search_admin():
            raise NotImplementedError("Search 2.0 administration is not "
                                      "supported for this version")
        msg_code = riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_GET_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_get_search_index(index)
        resp_code, resp = self._request(msg, codec)
        if len(resp.index) > 0:
            return codec.decode_search_index(resp.index[0])
        else:
            raise RiakError('notfound')

    def list_search_indexes(self):
        if not self.pb_search_admin():
            raise NotImplementedError("Search 2.0 administration is not "
                                      "supported for this version")
        msg_code = riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_GET_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_list_search_indexes()
        resp_code, resp = self._request(msg, codec)
        return [codec.decode_search_index(index) for index in resp.index]

    def delete_search_index(self, index):
        if not self.pb_search_admin():
            raise NotImplementedError("Search 2.0 administration is not "
                                      "supported for this version")
        msg_code = riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_DELETE_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_delete_search_index(index)
        self._request(msg, codec)
        return True

    def create_search_schema(self, schema, content):
        if not self.pb_search_admin():
            raise NotImplementedError("Search 2.0 administration is not "
                                      "supported for this version")
        msg_code = riak.pb.messages.MSG_CODE_YOKOZUNA_SCHEMA_PUT_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_create_search_schema(schema, content)
        self._request(msg, codec)
        return True

    def get_search_schema(self, schema):
        if not self.pb_search_admin():
            raise NotImplementedError("Search 2.0 administration is not "
                                      "supported for this version")
        msg_code = riak.pb.messages.MSG_CODE_YOKOZUNA_SCHEMA_GET_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_get_search_schema(schema)
        resp_code, resp = self._request(msg, codec)
        return codec.decode_get_search_schema(resp)

    def search(self, index, query, **kwargs):
        # TODO FUTURE NUKE THIS MAPRED
        if not self.pb_search():
            return self._search_mapred_emu(index, query)
        if six.PY2 and isinstance(query, unicode):  # noqa
            query = query.encode('utf8')
        msg_code = riak.pb.messages.MSG_CODE_SEARCH_QUERY_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_search(index, query, **kwargs)
        resp_code, resp = self._request(msg, codec)
        return codec.decode_search(resp)

    def get_counter(self, bucket, key, **kwargs):
        if not bucket.bucket_type.is_default():
            raise NotImplementedError("Counters are not "
                                      "supported with bucket-types, "
                                      "use datatypes instead.")
        if not self.counters():
            raise NotImplementedError("Counters are not supported")
        msg_code = riak.pb.messages.MSG_CODE_COUNTER_GET_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_get_counter(bucket, key, **kwargs)
        resp_code, resp = self._request(msg, codec)
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
        msg_code = riak.pb.messages.MSG_CODE_COUNTER_UPDATE_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_update_counter(bucket, key, value, **kwargs)
        resp_code, resp = self._request(msg, codec)
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
        msg_code = riak.pb.messages.MSG_CODE_DT_FETCH_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_fetch_datatype(bucket, key, **kwargs)
        resp_code, resp = self._request(msg, codec)
        return codec.decode_dt_fetch(resp)

    def update_datatype(self, datatype, **kwargs):
        if datatype.bucket.bucket_type.is_default():
            raise NotImplementedError("Datatypes cannot be used in the default"
                                      " bucket-type.")
        if not self.datatypes():
            raise NotImplementedError("Datatypes are not supported.")
        msg_code = riak.pb.messages.MSG_CODE_DT_UPDATE_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_update_datatype(datatype, **kwargs)
        resp_code, resp = self._request(msg, codec)
        codec.decode_update_datatype(datatype, resp, **kwargs)
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
        if not self.preflists():
            raise NotImplementedError("fetching preflists is not supported.")
        msg_code = riak.pb.messages.MSG_CODE_GET_BUCKET_KEY_PREFLIST_REQ
        codec = self._get_codec(msg_code)
        msg = codec.encode_get_preflist(bucket, key)
        resp_code, resp = self._request(msg, codec)
        return [codec.decode_preflist(item) for item in resp.preflist]

    def _request(self, msg, codec=None):
        if isinstance(msg, Msg):
            msg_code = msg.msg_code
            data = msg.data
            expect = msg.resp_code
        else:
            raise ValueError('expected a Msg argument')

        if not isinstance(codec, Codec):
            raise ValueError('expected a Codec argument')

        resp_code, data = self._send_recv(msg_code, data)
        # NB: decodes errors with msg code 0
        codec.maybe_riak_error(resp_code, data)
        codec.maybe_incorrect_code(resp_code, expect)
        if resp_code == MSG_CODE_TS_TTB_MSG or \
           resp_code in riak.pb.messages.MESSAGE_CLASSES:
            msg = codec.parse_msg(resp_code, data)
        else:
            # NB: raise a BadResource to ensure this connection is
            # closed and not re-used
            raise BadResource('unknown msg code {}'.format(resp_code))
        return resp_code, msg
