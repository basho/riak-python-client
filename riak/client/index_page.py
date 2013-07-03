"""
Copyright 2013 Basho Technologies, Inc.

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

from collections import namedtuple, Sequence


CONTINUATION = namedtuple('Continuation', ['c'])


class IndexPage(Sequence, object):
    """
    Encapsulates a single page of results from a secondary index
    query, with the ability to iterate over results (if not streamed),
    capture the page marker (continuation), and automatically fetch
    the next page.

    While users will interact with this object, it will be created
    automatically by the client and does not need to be instantiated
    elsewhere.
    """
    def __init__(self, client, bucket, index, startkey, endkey, return_terms,
                 max_results):
        self.client = client
        self.bucket = bucket
        self.index = index
        self.startkey = startkey
        self.endkey = endkey
        self.return_terms = return_terms
        self.max_results = max_results
        self.results = None
        self.continuation = None
        self.stream = False

    def __iter__(self):
        if self.results:
            try:
                for result in self.results:
                    if self.stream and isinstance(result, CONTINUATION):
                        self.continuation = result.c
                    else:
                        yield result
            finally:
                if self.stream:
                    self.results.close()
        else:
            raise ValueError("No index results to iterate")

    def __len__(self):
        if not self.stream and self.results is not None:
            return len(self.results)
        else:
            raise ValueError("Streamed index page has no length")

    def __getitem__(self, index):
        if not self.stream and self.results is not None:
            return self.results[index]
        else:
            raise ValueError("Streamed index page has no entries")

    def __eq__(self, other):
        if isinstance(other, list) and not (self.stream or
                                            self.results is None):
            return self.results == other
        elif isinstance(other, IndexPage):
            return other.__dict__ == self.__dict__
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def has_next_page(self):
        """
        Whether there is another page available, i.e. the response
        included a continuation.
        """
        return self.continuation is not None

    def next_page(self, stream=None):
        """
        Fetches the next page using the same parameters as the
        original query.

        Note that if streaming was used before, it will be used again
        unless overridden.

        :param stream: whether to enable streaming. `True` enables,
            `False` disables, `None` uses previous value.
        :type stream: boolean
        """
        if not self.continuation:
            raise ValueError("Cannot get next index page, no continuation")

        if stream is not None:
            self.stream = stream

        args = {'bucket': self.bucket,
                'index': self.index,
                'startkey': self.startkey,
                'endkey': self.endkey,
                'return_terms': self.return_terms,
                'max_results': self.max_results,
                'continuation': self.continuation}
        if self.stream:
            return self.client.stream_index(**args)
        else:
            return self.client.get_index(**args)
