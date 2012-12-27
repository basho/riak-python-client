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

try:
    import json
except ImportError:
    import simplejson as json

from riak.transports.pbc.messages import MSG_CODE_LIST_KEYS_RESP
from riak.transports.pbc.messages import MSG_CODE_MAPRED_RESP


class RiakPbcStream(class):
    """
    Used internally by RiakPbcTransport to implement streaming
    operations. Implements the iterator interface.
    """
    def __init__(self, transport):
        self.transport = transport

    def __iter__(self):
        return self

    def next(self):
        expect = self._expect
        try:
            resp = self.transport._recv_msg(expect)
            if(self._is_done(resp)):
                raise StopIteration
            else:
                return resp
        except StopIteration:
            pass
        except:
            # TODO: which exceptions do we expect to be generated?
            # Should we raise BadResource?
            raise StopIteration

    def _is_done(self, response):
        raise NotImplementedError


class RiakPbcKeyStream(RiakPbcStream):
    """
    Used internally by RiakPbcTransport to implement key-list streams.
    """

    _expect = MSG_CODE_LIST_KEYS_RESP

    def next(self):
        response = super(RiakPbcKeyStream, self).__next__()
        return response.keys

    def _is_done(self, response):
        return response.done


class RiakPbcMapredStream(RiakPbcStream):
    """
    Used internally by RiakPbcTransport to implement MapReduce
    streams.
    """

    _expect = MSG_CODE_MAPRED_RESP

    def next(self):
        response = super(RiakPbcMapredStream, self).next()
        return (response.phase, json.loads(response.response))

    def _is_done(self, response):
        return response.done
