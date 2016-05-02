import json
import re

from cgi import parse_header
from email import message_from_string
from riak.util import decode_index_value
from riak.client.index_page import CONTINUATION
from riak import RiakError
from six import PY2


class HttpStream(object):
    """
    Base class for HTTP streaming iterators.
    """

    BLOCK_SIZE = 2048

    def __init__(self, response):
        self.response = response
        self.buffer = ''
        self.response_done = False
        self.resource = None

    def __iter__(self):
        return self

    def _read(self):
        chunk = self.response.read(self.BLOCK_SIZE)
        if PY2:
            if chunk == '':
                self.response_done = True
            self.buffer += chunk
        else:
            if chunk == b'':
                self.response_done = True
            self.buffer += chunk.decode('utf-8')

    def __next__(self):
        raise NotImplementedError

    def next(self):
        raise NotImplementedError

    def attach(self, resource):
        self.resource = resource

    def close(self):
        self.resource.release()


class HttpJsonStream(HttpStream):
    _json_field = None

    def next(self):
        # Python 2.x Version
        while '}' not in self.buffer and not self.response_done:
            self._read()

        if '}' in self.buffer:
            idx = self.buffer.index('}') + 1
            chunk = self.buffer[:idx]
            self.buffer = self.buffer[idx:]
            jsdict = json.loads(chunk)
            if 'error' in jsdict:
                self.close()
                raise RiakError(jsdict['error'])
            field = jsdict[self._json_field]
            return field
        else:
            raise StopIteration

    def __next__(self):
        # Python 3.x Version
        return self.next()


class HttpKeyStream(HttpJsonStream):
    """
    Streaming iterator for list-keys over HTTP
    """
    _json_field = u'keys'


class HttpBucketStream(HttpJsonStream):
    """
    Streaming iterator for list-buckets over HTTP
    """
    _json_field = u'buckets'


class HttpMultipartStream(HttpStream):
    """
    Streaming iterator for multipart messages over HTTP
    """
    def __init__(self, response):
        super(HttpMultipartStream, self).__init__(response)
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

    def __next__(self):
        # Python 3.x Version
        return self.next()

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

    def next(self):
        message = super(HttpMapReduceStream, self).next()
        payload = json.loads(message.get_payload())
        return payload['phase'], payload['data']

    def __next__(self):
        # Python 3.x Version
        return self.next()


class HttpIndexStream(HttpMultipartStream):
    """
    Streaming iterator for secondary indexes over HTTP
    """

    def __init__(self, response, index, return_terms):
        super(HttpIndexStream, self).__init__(response)
        self.index = index
        self.return_terms = return_terms

    def next(self):
        message = super(HttpIndexStream, self).next()
        payload = json.loads(message.get_payload())
        if u'error' in payload:
            raise RiakError(payload[u'error'])
        elif u'keys' in payload:
            return payload[u'keys']
        elif u'results' in payload:
            structs = payload[u'results']
            # Format is {"results":[{"2ikey":"primarykey"}, ...]}
            return [self._decode_pair(list(d.items())[0]) for d in structs]
        elif u'continuation' in payload:
            return CONTINUATION(payload[u'continuation'])

    def __next__(self):
        # Python 3.x Version
        return self.next()

    def _decode_pair(self, pair):
        return (decode_index_value(self.index, pair[0]), pair[1])
