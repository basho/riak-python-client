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


import json
from riak_pb.messages import (
    MSG_CODE_LIST_KEYS_RESP,
    MSG_CODE_MAP_RED_RESP,
    MSG_CODE_LIST_BUCKETS_RESP,
    MSG_CODE_INDEX_RESP
)
from riak.util import decode_index_value, bytes_to_str
from riak.client.index_page import CONTINUATION
from six import PY2


class RiakPbcStream(object):
    """
    Used internally by RiakPbcTransport to implement streaming
    operations. Implements the iterator interface.
    """

    _expect = None

    def __init__(self, transport):
        self.finished = False
        self.transport = transport
        self.resource = None

    def __iter__(self):
        return self

    def next(self):
        if self.finished:
            raise StopIteration

        try:
            msg_code, resp = self.transport._recv_msg(expect=self._expect)
        except:
            self.finished = True
            raise

        if(self._is_done(resp)):
            self.finished = True

        return resp

    def __next__(self):
        # Python 3.x Version
        return self.next()

    def _is_done(self, response):
        # This could break if new messages don't name the field the
        # same thing.
        return response.done

    def attach(self, resource):
        self.resource = resource

    def close(self):
        # We have to drain the socket to make sure that we don't get
        # weird responses when some other request comes after a
        # failed/prematurely-terminated one.
        try:
            while self.next():
                pass
        except StopIteration:
            pass
        self.resource.release()


class RiakPbcKeyStream(RiakPbcStream):
    """
    Used internally by RiakPbcTransport to implement key-list streams.
    """

    _expect = MSG_CODE_LIST_KEYS_RESP

    def next(self):
        response = super(RiakPbcKeyStream, self).next()

        if response.done and len(response.keys) is 0:
            raise StopIteration

        return response.keys

    def __next__(self):
        # Python 3.x Version
        return self.next()


class RiakPbcMapredStream(RiakPbcStream):
    """
    Used internally by RiakPbcTransport to implement MapReduce
    streams.
    """

    _expect = MSG_CODE_MAP_RED_RESP

    def next(self):
        response = super(RiakPbcMapredStream, self).next()

        if response.done and not response.HasField('response'):
            raise StopIteration

        return response.phase, json.loads(bytes_to_str(response.response))

    def __next__(self):
        # Python 3.x Version
        return self.next()


class RiakPbcBucketStream(RiakPbcStream):
    """
    Used internally by RiakPbcTransport to implement key-list streams.
    """

    _expect = MSG_CODE_LIST_BUCKETS_RESP

    def next(self):
        response = super(RiakPbcBucketStream, self).next()

        if response.done and len(response.buckets) is 0:
            raise StopIteration

        return response.buckets

    def __next__(self):
        # Python 3.x Version
        return self.next()


class RiakPbcIndexStream(RiakPbcStream):
    """
    Used internally by RiakPbcTransport to implement Secondary Index
    streams.
    """

    _expect = MSG_CODE_INDEX_RESP

    def __init__(self, transport, index, return_terms=False):
        super(RiakPbcIndexStream, self).__init__(transport)
        self.index = index
        self.return_terms = return_terms

    def next(self):
        response = super(RiakPbcIndexStream, self).next()

        if response.done and not (response.keys or
                                  response.results or
                                  response.continuation):
            raise StopIteration

        if self.return_terms and response.results:
            return [(decode_index_value(self.index, r.key),
                     bytes_to_str(r.value))
                    for r in response.results]
        elif response.keys:
            if PY2:
                return response.keys[:]
            else:
                return [bytes_to_str(key) for key in response.keys]
        elif response.continuation:
            return CONTINUATION(bytes_to_str(response.continuation))

    def __next__(self):
        # Python 3.x Version
        return self.next()
