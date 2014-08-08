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
                 max_results, term_regex):
        self.client = client
        self.bucket = bucket
        self.index = index
        self.startkey = startkey
        self.endkey = endkey
        self.return_terms = return_terms
        self.max_results = max_results
        self.results = None
        self.stream = False
        self.term_regex = term_regex

    continuation = None
    """
    The opaque page marker that is used when fetching the next chunk
    of results. The user can simply call :meth:`next_page` to do so,
    or pass this to the :meth:`~riak.client.RiakClient.get_index`
    method using the ``continuation`` option.
    """

    def __iter__(self):
        """
        Emulates the iterator interface. When streaming, this means
        delegating to the stream, otherwise iterating over the
        existing result set.
        """
        if self.results is None:
            raise ValueError("No index results to iterate")

        try:
            for result in self.results:
                if self.stream and isinstance(result, CONTINUATION):
                    self.continuation = result.c
                else:
                    yield self._inject_term(result)
        finally:
            if self.stream:
                self.results.close()

    def __len__(self):
        """
        Returns the length of the captured results.
        """
        if self._has_results():
            return len(self.results)
        else:
            raise ValueError("Streamed index page has no length")

    def __getitem__(self, index):
        """
        Fetches an item by index from the captured results.
        """
        if self._has_results():
            return self.results[index]
        else:
            raise ValueError("Streamed index page has no entries")

    def __eq__(self, other):
        """
        An IndexPage can pretend to be equal to a list when it has
        captured results by simply comparing the internal results to
        the passed list. Otherwise the other object needs to be an
        equivalent IndexPage.
        """
        if isinstance(other, list) and self._has_results():
            return self._inject_term(self.results) == other
        elif isinstance(other, IndexPage):
            return other.__dict__ == self.__dict__
        else:
            return False

    def __ne__(self, other):
        """
        Converse of __eq__.
        """
        return not self.__eq__(other)

    def has_next_page(self):
        """
        Whether there is another page available, i.e. the response
        included a continuation.
        """
        return self.continuation is not None

    def next_page(self, timeout=None, stream=None):
        """
        Fetches the next page using the same parameters as the
        original query.

        Note that if streaming was used before, it will be used again
        unless overridden.

        :param stream: whether to enable streaming. `True` enables,
            `False` disables, `None` uses previous value.
        :type stream: boolean
        :param timeout: a timeout value in milliseconds, or 'infinity'
        :type timeout: int
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
                'continuation': self.continuation,
                'timeout': timeout,
                'term_regex': self.term_regex}

        if self.stream:
            return self.client.stream_index(**args)
        else:
            return self.client.get_index(**args)

    def _has_results(self):
        """
        When not streaming, have results been assigned?
        """
        return not (self.stream or self.results is None)

    def _should_inject_term(self, term):
        """
        The index term should be injected when using an equality query
        and the return terms option. If the term is already a tuple,
        it can be skipped.
        """
        return self.return_terms and not self.endkey

    def _inject_term(self, result):
        """
        Upgrades a result (streamed or not) to include the index term
        when an equality query is used with return_terms.
        """
        if self._should_inject_term(result):
            if type(result) is list:
                return [(self.startkey, r) for r in result]
            else:
                return (self.startkey, result)
        else:
            return result

    def __repr__(self):
        return "<{!s} {!r}>".format(self.__class__.__name__, self.__dict__)

    def close(self):
        if self.stream:
            self.results.close()
