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
import string
import re
from cgi import parse_header
from email import message_from_string
from riak.util import decode_index_value
from riak.client.index_page import CONTINUATION
from riak import RiakError


class RiakHttpStream(object):
    """
    Base class for HTTP streaming iterators.
    """

    BLOCK_SIZE = 2048

    def __init__(self, response):
        self.response = response
        self.buffer = ''
        self.response_done = False

    def __iter__(self):
        return self

    def _read(self):
        chunk = self.response.read(self.BLOCK_SIZE)
        if chunk == '':
            self.response_done = True
        self.buffer += chunk

    def next(self):
        raise NotImplementedError

    def close(self):
        pass


class RiakHttpJsonStream(RiakHttpStream):
    _json_field = None

    def next(self):
        while '}' not in self.buffer and not self.response_done:
            self._read()

        if '}' in self.buffer:
            idx = string.index(self.buffer, '}') + 1
            chunk = self.buffer[:idx]
            self.buffer = self.buffer[idx:]
            field = json.loads(chunk)[self._json_field]
            return field
        else:
            raise StopIteration


class RiakHttpKeyStream(RiakHttpJsonStream):
    """
    Streaming iterator for list-keys over HTTP
    """
    _json_field = u'keys'


class RiakHttpBucketStream(RiakHttpJsonStream):
    """
    Streaming iterator for list-buckets over HTTP
    """
    _json_field = u'buckets'


class RiakHttpMultipartStream(RiakHttpStream):
    """
    Streaming iterator for multipart messages over HTTP
    """
    def __init__(self, response):
        super(RiakHttpMultipartStream, self).__init__(response)
        ctypehdr = response.getheader('content-type')
        _, params = parse_header(ctypehdr)
        self.boundary_re = re.compile('\r?\n--%s(?:--)?\r?\n' %
                                      re.escape(params['boundary']))
        self.next_boundary = None
        self.seen_first = False

    def next(self):
        # multipart/mixed starts with a boundary, then the first part.
        if not self.seen_first:
            self.read_until_boundary()
            self.advance_buffer()
            self.seen_first = True

        self.read_until_boundary()

        if self.next_boundary:
            part = self.advance_buffer()
            message = message_from_string(part)
            return message
        else:
            raise StopIteration

    def try_match(self):
        self.next_boundary = self.boundary_re.search(self.buffer)
        return self.next_boundary

    def advance_buffer(self):
        part = self.buffer[:self.next_boundary.start()]
        self.buffer = self.buffer[self.next_boundary.end():]
        self.next_boundary = None
        return part

    def read_until_boundary(self):
        while not self.try_match() and not self.response_done:
            self._read()


class RiakHttpMapReduceStream(RiakHttpMultipartStream):
    """
    Streaming iterator for MapReduce over HTTP
    """

    def next(self):
        message = super(RiakHttpMapReduceStream, self).next()
        payload = json.loads(message.get_payload())
        return payload['phase'], payload['data']


class RiakHttpIndexStream(RiakHttpMultipartStream):
    """
    Streaming iterator for secondary indexes over HTTP
    """

    def __init__(self, response, index, return_terms):
        super(RiakHttpIndexStream, self).__init__(response)
        self.index = index
        self.return_terms = return_terms

    def next(self):
        message = super(RiakHttpIndexStream, self).next()
        payload = json.loads(message.get_payload())
        if u'error' in payload:
            raise RiakError(payload[u'error'])
        elif u'keys' in payload:
            return payload[u'keys']
        elif u'results' in payload:
            structs = payload[u'results']
            # Format is {"results":[{"2ikey":"primarykey"}, ...]}
            return [self._decode_pair(d.items()[0]) for d in structs]
        elif u'continuation' in payload:
            return CONTINUATION(payload[u'continuation'])

    def _decode_pair(self, pair):
        return (decode_index_value(self.index, pair[0]), pair[1])
