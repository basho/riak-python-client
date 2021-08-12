# This is a generated file. DO NOT EDIT.

"""
Constants and mappings between Riak protocol codes and messages.
"""

import riak.pb.riak_dt_pb2
import riak.pb.riak_kv_pb2
import riak.pb.riak_pb2
import riak.pb.riak_search_pb2
import riak.pb.riak_ts_pb2
import riak.pb.riak_yokozuna_pb2

# Protocol codes
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
MSG_CODE_MAP_RED_REQ = 23
MSG_CODE_MAP_RED_RESP = 24
MSG_CODE_INDEX_REQ = 25
MSG_CODE_INDEX_RESP = 26
MSG_CODE_SEARCH_QUERY_REQ = 27
MSG_CODE_SEARCH_QUERY_RESP = 28
MSG_CODE_RESET_BUCKET_REQ = 29
MSG_CODE_RESET_BUCKET_RESP = 30
MSG_CODE_GET_BUCKET_TYPE_REQ = 31
MSG_CODE_SET_BUCKET_TYPE_REQ = 32
MSG_CODE_GET_BUCKET_KEY_PREFLIST_REQ = 33
MSG_CODE_GET_BUCKET_KEY_PREFLIST_RESP = 34
MSG_CODE_CS_BUCKET_REQ = 40
MSG_CODE_CS_BUCKET_RESP = 41
MSG_CODE_INDEX_BODY_RESP = 42
MSG_CODE_COUNTER_UPDATE_REQ = 50
MSG_CODE_COUNTER_UPDATE_RESP = 51
MSG_CODE_COUNTER_GET_REQ = 52
MSG_CODE_COUNTER_GET_RESP = 53
MSG_CODE_YOKOZUNA_INDEX_GET_REQ = 54
MSG_CODE_YOKOZUNA_INDEX_GET_RESP = 55
MSG_CODE_YOKOZUNA_INDEX_PUT_REQ = 56
MSG_CODE_YOKOZUNA_INDEX_DELETE_REQ = 57
MSG_CODE_YOKOZUNA_SCHEMA_GET_REQ = 58
MSG_CODE_YOKOZUNA_SCHEMA_GET_RESP = 59
MSG_CODE_YOKOZUNA_SCHEMA_PUT_REQ = 60
MSG_CODE_COVERAGE_REQ = 70
MSG_CODE_COVERAGE_RESP = 71
MSG_CODE_DT_FETCH_REQ = 80
MSG_CODE_DT_FETCH_RESP = 81
MSG_CODE_DT_UPDATE_REQ = 82
MSG_CODE_DT_UPDATE_RESP = 83
MSG_CODE_TS_QUERY_REQ = 90
MSG_CODE_TS_QUERY_RESP = 91
MSG_CODE_TS_PUT_REQ = 92
MSG_CODE_TS_PUT_RESP = 93
MSG_CODE_TS_DEL_REQ = 94
MSG_CODE_TS_DEL_RESP = 95
MSG_CODE_TS_GET_REQ = 96
MSG_CODE_TS_GET_RESP = 97
MSG_CODE_TS_LIST_KEYS_REQ = 98
MSG_CODE_TS_LIST_KEYS_RESP = 99
MSG_CODE_TS_COVERAGE_REQ = 100
MSG_CODE_TS_COVERAGE_RESP = 101
MSG_CODE_TS_COVERAGE_ENTRY = 102
MSG_CODE_TS_RANGE = 103
MSG_CODE_TS_TTB_MSG = 104
MSG_CODE_AUTH_REQ = 253
MSG_CODE_AUTH_RESP = 254
MSG_CODE_START_TLS = 255

# Mapping from code to protobuf class
MESSAGE_CLASSES = {
    MSG_CODE_ERROR_RESP: riak.pb.riak_pb2.RpbErrorResp,
    MSG_CODE_PING_REQ: None,
    MSG_CODE_PING_RESP: None,
    MSG_CODE_GET_CLIENT_ID_REQ: None,
    MSG_CODE_GET_CLIENT_ID_RESP: riak.pb.riak_kv_pb2.RpbGetClientIdResp,
    MSG_CODE_SET_CLIENT_ID_REQ: riak.pb.riak_kv_pb2.RpbSetClientIdReq,
    MSG_CODE_SET_CLIENT_ID_RESP: None,
    MSG_CODE_GET_SERVER_INFO_REQ: None,
    MSG_CODE_GET_SERVER_INFO_RESP: riak.pb.riak_pb2.RpbGetServerInfoResp,
    MSG_CODE_GET_REQ: riak.pb.riak_kv_pb2.RpbGetReq,
    MSG_CODE_GET_RESP: riak.pb.riak_kv_pb2.RpbGetResp,
    MSG_CODE_PUT_REQ: riak.pb.riak_kv_pb2.RpbPutReq,
    MSG_CODE_PUT_RESP: riak.pb.riak_kv_pb2.RpbPutResp,
    MSG_CODE_DEL_REQ: riak.pb.riak_kv_pb2.RpbDelReq,
    MSG_CODE_DEL_RESP: None,
    MSG_CODE_LIST_BUCKETS_REQ: riak.pb.riak_kv_pb2.RpbListBucketsReq,
    MSG_CODE_LIST_BUCKETS_RESP: riak.pb.riak_kv_pb2.RpbListBucketsResp,
    MSG_CODE_LIST_KEYS_REQ: riak.pb.riak_kv_pb2.RpbListKeysReq,
    MSG_CODE_LIST_KEYS_RESP: riak.pb.riak_kv_pb2.RpbListKeysResp,
    MSG_CODE_GET_BUCKET_REQ: riak.pb.riak_pb2.RpbGetBucketReq,
    MSG_CODE_GET_BUCKET_RESP: riak.pb.riak_pb2.RpbGetBucketResp,
    MSG_CODE_SET_BUCKET_REQ: riak.pb.riak_pb2.RpbSetBucketReq,
    MSG_CODE_SET_BUCKET_RESP: None,
    MSG_CODE_MAP_RED_REQ: riak.pb.riak_kv_pb2.RpbMapRedReq,
    MSG_CODE_MAP_RED_RESP: riak.pb.riak_kv_pb2.RpbMapRedResp,
    MSG_CODE_INDEX_REQ: riak.pb.riak_kv_pb2.RpbIndexReq,
    MSG_CODE_INDEX_RESP: riak.pb.riak_kv_pb2.RpbIndexResp,
    MSG_CODE_SEARCH_QUERY_REQ: riak.pb.riak_search_pb2.RpbSearchQueryReq,
    MSG_CODE_SEARCH_QUERY_RESP: riak.pb.riak_search_pb2.RpbSearchQueryResp,
    MSG_CODE_RESET_BUCKET_REQ: riak.pb.riak_pb2.RpbResetBucketReq,
    MSG_CODE_RESET_BUCKET_RESP: None,
    MSG_CODE_GET_BUCKET_TYPE_REQ: riak.pb.riak_pb2.RpbGetBucketTypeReq,
    MSG_CODE_SET_BUCKET_TYPE_REQ: riak.pb.riak_pb2.RpbSetBucketTypeReq,
    MSG_CODE_GET_BUCKET_KEY_PREFLIST_REQ:
    riak.pb.riak_kv_pb2.RpbGetBucketKeyPreflistReq,
    MSG_CODE_GET_BUCKET_KEY_PREFLIST_RESP:
    riak.pb.riak_kv_pb2.RpbGetBucketKeyPreflistResp,
    MSG_CODE_CS_BUCKET_REQ: riak.pb.riak_kv_pb2.RpbCSBucketReq,
    MSG_CODE_CS_BUCKET_RESP: riak.pb.riak_kv_pb2.RpbCSBucketResp,
    MSG_CODE_INDEX_BODY_RESP: riak.pb.riak_kv_pb2.RpbIndexBodyResp,
    MSG_CODE_COUNTER_UPDATE_REQ: riak.pb.riak_kv_pb2.RpbCounterUpdateReq,
    MSG_CODE_COUNTER_UPDATE_RESP: riak.pb.riak_kv_pb2.RpbCounterUpdateResp,
    MSG_CODE_COUNTER_GET_REQ: riak.pb.riak_kv_pb2.RpbCounterGetReq,
    MSG_CODE_COUNTER_GET_RESP: riak.pb.riak_kv_pb2.RpbCounterGetResp,
    MSG_CODE_YOKOZUNA_INDEX_GET_REQ:
    riak.pb.riak_yokozuna_pb2.RpbYokozunaIndexGetReq,
    MSG_CODE_YOKOZUNA_INDEX_GET_RESP:
    riak.pb.riak_yokozuna_pb2.RpbYokozunaIndexGetResp,
    MSG_CODE_YOKOZUNA_INDEX_PUT_REQ:
    riak.pb.riak_yokozuna_pb2.RpbYokozunaIndexPutReq,
    MSG_CODE_YOKOZUNA_INDEX_DELETE_REQ:
    riak.pb.riak_yokozuna_pb2.RpbYokozunaIndexDeleteReq,
    MSG_CODE_YOKOZUNA_SCHEMA_GET_REQ:
    riak.pb.riak_yokozuna_pb2.RpbYokozunaSchemaGetReq,
    MSG_CODE_YOKOZUNA_SCHEMA_GET_RESP:
    riak.pb.riak_yokozuna_pb2.RpbYokozunaSchemaGetResp,
    MSG_CODE_YOKOZUNA_SCHEMA_PUT_REQ:
    riak.pb.riak_yokozuna_pb2.RpbYokozunaSchemaPutReq,
    MSG_CODE_COVERAGE_REQ: riak.pb.riak_kv_pb2.RpbCoverageReq,
    MSG_CODE_COVERAGE_RESP: riak.pb.riak_kv_pb2.RpbCoverageResp,
    MSG_CODE_DT_FETCH_REQ: riak.pb.riak_dt_pb2.DtFetchReq,
    MSG_CODE_DT_FETCH_RESP: riak.pb.riak_dt_pb2.DtFetchResp,
    MSG_CODE_DT_UPDATE_REQ: riak.pb.riak_dt_pb2.DtUpdateReq,
    MSG_CODE_DT_UPDATE_RESP: riak.pb.riak_dt_pb2.DtUpdateResp,
    MSG_CODE_TS_QUERY_REQ: riak.pb.riak_ts_pb2.TsQueryReq,
    MSG_CODE_TS_QUERY_RESP: riak.pb.riak_ts_pb2.TsQueryResp,
    MSG_CODE_TS_PUT_REQ: riak.pb.riak_ts_pb2.TsPutReq,
    MSG_CODE_TS_PUT_RESP: riak.pb.riak_ts_pb2.TsPutResp,
    MSG_CODE_TS_DEL_REQ: riak.pb.riak_ts_pb2.TsDelReq,
    MSG_CODE_TS_DEL_RESP: riak.pb.riak_ts_pb2.TsDelResp,
    MSG_CODE_TS_GET_REQ: riak.pb.riak_ts_pb2.TsGetReq,
    MSG_CODE_TS_GET_RESP: riak.pb.riak_ts_pb2.TsGetResp,
    MSG_CODE_TS_LIST_KEYS_REQ: riak.pb.riak_ts_pb2.TsListKeysReq,
    MSG_CODE_TS_LIST_KEYS_RESP: riak.pb.riak_ts_pb2.TsListKeysResp,
    MSG_CODE_TS_COVERAGE_REQ: riak.pb.riak_ts_pb2.TsCoverageReq,
    MSG_CODE_TS_COVERAGE_RESP: riak.pb.riak_ts_pb2.TsCoverageResp,
    MSG_CODE_TS_COVERAGE_ENTRY: riak.pb.riak_ts_pb2.TsCoverageEntry,
    MSG_CODE_TS_RANGE: riak.pb.riak_ts_pb2.TsRange,
    MSG_CODE_TS_TTB_MSG: None,
    MSG_CODE_AUTH_REQ: riak.pb.riak_pb2.RpbAuthReq,
    MSG_CODE_AUTH_RESP: None,
    MSG_CODE_START_TLS: None
}
