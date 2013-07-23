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

import riak_pb


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
MSG_CODE_MAPRED_REQ = 23
MSG_CODE_MAPRED_RESP = 24
MSG_CODE_INDEX_REQ = 25
MSG_CODE_INDEX_RESP = 26
MSG_CODE_SEARCH_QUERY_REQ = 27
MSG_CODE_SEARCH_QUERY_RESP = 28
MSG_CODE_RESET_BUCKET_REQ = 29
MSG_CODE_RESET_BUCKET_RESP = 30
MSG_CODE_COUNTER_UPDATE_REQ = 50
MSG_CODE_COUNTER_UPDATE_RESP = 51
MSG_CODE_COUNTER_GET_REQ = 52
MSG_CODE_COUNTER_GET_RESP = 53

# These responses don't include messages
EMPTY_RESPONSES = [
    MSG_CODE_PING_RESP,
    MSG_CODE_SET_CLIENT_ID_RESP,
    MSG_CODE_DEL_RESP,
    MSG_CODE_SET_BUCKET_RESP,
    MSG_CODE_RESET_BUCKET_RESP
]

# Mapping from code to protobuf class
MESSAGE_CLASSES = {
    MSG_CODE_ERROR_RESP: riak_pb.RpbErrorResp,
    MSG_CODE_PING_REQ: None,
    MSG_CODE_PING_RESP: None,
    MSG_CODE_GET_CLIENT_ID_REQ: None,
    MSG_CODE_GET_CLIENT_ID_RESP: riak_pb.RpbGetClientIdResp,
    MSG_CODE_SET_CLIENT_ID_REQ: riak_pb.RpbSetClientIdReq,
    MSG_CODE_SET_CLIENT_ID_RESP: None,
    MSG_CODE_GET_SERVER_INFO_REQ: None,
    MSG_CODE_GET_SERVER_INFO_RESP: riak_pb.RpbGetServerInfoResp,
    MSG_CODE_GET_REQ: riak_pb.RpbGetReq,
    MSG_CODE_GET_RESP: riak_pb.RpbGetResp,
    MSG_CODE_PUT_REQ: riak_pb.RpbPutReq,
    MSG_CODE_PUT_RESP: riak_pb.RpbPutResp,
    MSG_CODE_DEL_REQ: riak_pb.RpbDelReq,
    MSG_CODE_DEL_RESP: None,
    MSG_CODE_LIST_BUCKETS_REQ: riak_pb.RpbListBucketsReq,
    MSG_CODE_LIST_BUCKETS_RESP: riak_pb.RpbListBucketsResp,
    MSG_CODE_LIST_KEYS_REQ: riak_pb.RpbListKeysReq,
    MSG_CODE_LIST_KEYS_RESP: riak_pb.RpbListKeysResp,
    MSG_CODE_GET_BUCKET_REQ: riak_pb.RpbGetBucketReq,
    MSG_CODE_GET_BUCKET_RESP: riak_pb.RpbGetBucketResp,
    MSG_CODE_SET_BUCKET_REQ: riak_pb.RpbSetBucketReq,
    MSG_CODE_SET_BUCKET_RESP: None,
    MSG_CODE_MAPRED_REQ: riak_pb.RpbMapRedReq,
    MSG_CODE_MAPRED_RESP: riak_pb.RpbMapRedResp,
    MSG_CODE_INDEX_REQ: riak_pb.RpbIndexReq,
    MSG_CODE_INDEX_RESP: riak_pb.RpbIndexResp,
    MSG_CODE_SEARCH_QUERY_REQ: riak_pb.RpbSearchQueryReq,
    MSG_CODE_SEARCH_QUERY_RESP: riak_pb.RpbSearchQueryResp,
    MSG_CODE_RESET_BUCKET_REQ: riak_pb.RpbResetBucketReq,
    MSG_CODE_RESET_BUCKET_RESP: None,
    MSG_CODE_COUNTER_UPDATE_REQ: riak_pb.RpbCounterUpdateReq,
    MSG_CODE_COUNTER_UPDATE_RESP: riak_pb.RpbCounterUpdateResp,
    MSG_CODE_COUNTER_GET_REQ: riak_pb.RpbCounterGetReq,
    MSG_CODE_COUNTER_GET_RESP: riak_pb.RpbCounterGetResp

}
