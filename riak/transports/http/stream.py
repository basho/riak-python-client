# Copyright 2010-present Basho Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import re

from cgi import parse_header
from email import message_from_string

from riak import RiakError
from riak.client.index_page import CONTINUATION
from riak.util import decode_index_value


class HttpStream(object):
    """
    Base class for HTTP streaming iterators.
    """

    BLOCK_SIZE = 2048

    def __init__(self, response):
        self.response = response
        self.buffer = ""
        self.response_done = False
        self.resource = None

    def __iter__(self):
        return self

    def _read(self):
        chunk = self.response.read(self.BLOCK_SIZE)
        if chunk == b'':
            self.response_done = True
        self.buffer += chunk.decode("utf-8")

    def __next__(self):
        raise NotImplementedError

    def attach(self, resource):
        self.resource = resource

    def close(self):
        self.resource.release()


class HttpJsonStream(HttpStream):
    _json_field = None

    def __next__(self):
        # Python 2.x Version
        while "}" not in self.buffer and not self.response_done:
            self._read()

        if "}" in self.buffer:
            idx = self.buffer.index("}") + 1
            chunk = self.buffer[:idx]
            self.buffer = self.buffer[idx:]
            jsdict = json.loads(chunk)
            if "error" in jsdict:
                self.close()
                raise RiakError(jsdict["error"])
            field = jsdict[self._json_field]
            return field
        else:
            raise StopIteration


class HttpKeyStream(HttpJsonStream):
    """
    Streaming iterator for list-keys over HTTP
    """
    _json_field = "keys"


class HttpBucketStream(HttpJsonStream):
    """
    Streaming iterator for list-buckets over HTTP
    """
    _json_field = "buckets"


class HttpMultipartStream(HttpStream):
    """
    Streaming iterator for multipart messages over HTTP
    """
    def __init__(self, response):
        super(HttpMultipartStream, self).__init__(response)
        ctypehdr = response.getheader("content-type")
        _, params = parse_header(ctypehdr)
        self.boundary_re = re.compile("\r?\n--%s(?:--)?\r?\n" %
                                      re.escape(params["boundary"]))
        self.next_boundary = None
        self.seen_first = False

    def __next__(self):
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


class HttpMapReduceStream(HttpMultipartStream):
    """
    Streaming iterator for MapReduce over HTTP
    """

    def __next__(self):
        message = super(HttpMapReduceStream, self).__next__()
        payload = json.loads(message.get_payload())
        return payload["phase"], payload["data"]


class HttpIndexStream(HttpMultipartStream):
    """
    Streaming iterator for secondary indexes over HTTP
    """

    def __init__(self, response, index, return_terms):
        super(HttpIndexStream, self).__init__(response)
        self.index = index
        self.return_terms = return_terms

    def __next__(self):
        message = super(HttpIndexStream, self).__next__()
        payload = json.loads(message.get_payload())
        if "error" in payload:
            raise RiakError(payload["error"])
        elif "keys" in payload:
            return payload["keys"]
        elif "results" in payload:
            structs = payload["results"]
            # Format is {"results":[{"2ikey":"primarykey"}, ...]}
            return [self._decode_pair(list(d.items())[0]) for d in structs]
        elif "continuation" in payload:
            return CONTINUATION(payload["continuation"])

    def _decode_pair(self, pair):
        return (decode_index_value(self.index, pair[0]), pair[1])
