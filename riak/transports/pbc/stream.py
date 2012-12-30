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
from riak.transports.pbc.messages import MSG_CODE_LIST_KEYS_RESP
from riak.transports.pbc.messages import MSG_CODE_MAPRED_RESP


class RiakPbcStream(object):
    """
    Used internally by RiakPbcTransport to implement streaming
    operations. Implements the iterator interface.
    """

    _expect = None

    def __init__(self, transport):
        self.finished = False
        self.transport = transport

    def __iter__(self):
        return self

    def next(self):
        if self.finished:
            raise StopIteration

        msg_code, resp = self.transport._recv_msg(self._expect)
        if(self._is_done(resp)):
            self.finished = True

        return resp

    def _is_done(self, response):
        # This could break if new messages don't name the field the
        # same thing.
        return response.done


class RiakPbcKeyStream(RiakPbcStream):
    """
    Used internally by RiakPbcTransport to implement key-list streams.
    """

    _expect = MSG_CODE_LIST_KEYS_RESP

    def next(self):
        response = super(RiakPbcKeyStream, self).next()

        if response.done and not response.HasField('keys'):
            raise StopIteration

        return response.keys


class RiakPbcMapredStream(RiakPbcStream):
    """
    Used internally by RiakPbcTransport to implement MapReduce
    streams.
    """

    _expect = MSG_CODE_MAPRED_RESP

    def next(self):
        response = super(RiakPbcMapredStream, self).next()

        if response.done and not response.HasField('response'):
            raise StopIteration

        return response.phase, json.loads(response.response)
