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

    def read(self):
        chunk = self.response.read(self.BLOCK_SIZE)
        if chunk is '':
            self.response_done = True
        self.buffer += chunk

    def next(self):
        raise NotImplementedError

class RiakHttpKeyStream(RiakHttpStream):
    """
    Streaming iterator for list-keys over HTTP
    """

    def next(self):
        while '}' not in self.buffer and not self.response_done:
            self.read()

        if '}' in self.buffer:
            idx = string.index(self.buffer, '}') + 1
            chunk = self.buffer[:idx]
            self.buffer = self.buffer[idx:]
            keys = json.loads(chunk)[u'keys']
            return keys
        else:
            raise StopIteration
