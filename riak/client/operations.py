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

from transport import RiakClientTransport, retryable, retryableHttpOnly
from multiget import multiget
from index_page import IndexPage


class RiakClientOperations(RiakClientTransport):
    """
    Methods for RiakClient that result in requests sent to the Riak
    cluster.

    Note that many of these methods have an implicit 'transport'
    argument that will be prepended automatically as part of the retry
    logic, and does not need to be supplied by the user.
    """

    @retryable
    def get_buckets(self, transport, timeout=None):
        """
        get_buckets(timeout=None)

        Get the list of buckets as :class:`RiakBucket
        <riak.bucket.RiakBucket>` instances.

        .. warning:: Do not use this in production, as it requires
           traversing through all keys stored in a cluster.

        .. note:: This request is automatically retried :attr:`RETRY_COUNT`
           times if it fails due to network error.

        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :rtype: list of :class:`RiakBucket <riak.bucket.RiakBucket>` instances
        """
        _validate_timeout(timeout)
        return [self.bucket(name) for name in
                transport.get_buckets(timeout=timeout)]

    def stream_buckets(self, timeout=None):
        """
        Streams the list of buckets. This is a generator method that
        should be iterated over.

        .. warning:: Do not use this in production, as it requires
           traversing through all keys stored in a cluster.

        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :rtype: iterator that yields lists of :class:`RiakBucket
             <riak.bucket.RiakBucket>` instances
        """
        _validate_timeout(timeout)
        with self._transport() as transport:
            stream = transport.stream_buckets(timeout=timeout)
            try:
                for bucket_list in stream:
                    bucket_list = [self.bucket(name) for name in bucket_list]
                    if len(bucket_list) > 0:
                        yield bucket_list
            finally:
                stream.close()

    @retryable
    def ping(self, transport):
        """
        ping()

        Check if the Riak server for this ``RiakClient`` instance is alive.

        .. note:: This request is automatically retried :attr:`RETRY_COUNT`
           times if it fails due to network error.

        :rtype: boolean
        """
        return transport.ping()

    is_alive = ping

    @retryable
    def get_index(self, transport, bucket, index, startkey, endkey=None,
                  return_terms=None, max_results=None, continuation=None,
                  timeout=None):
        """
        get_index(bucket, index, startkey, endkey=None, return_terms=None,\
                  max_results=None, continuation=None)

        Queries a secondary index, returning matching keys.

        .. note:: This request is automatically retried :attr:`RETRY_COUNT`
           times if it fails due to network error.

        :param bucket: the bucket whose index will be queried
        :type bucket: RiakBucket
        :param index: the index to query
        :type index: string
        :param startkey: the sole key to query, or beginning of the query range
        :type startkey: string, integer
        :param endkey: the end of the query range (optional if equality)
        :type endkey: string, integer
        :param return_terms: whether to include the secondary index value
        :type return_terms: boolean
        :param max_results: the maximum number of results to return (page size)
        :type max_results: integer
        :param continuation: the opaque continuation returned from a
            previous paginated request
        :type continuation: string
        :param timeout: a timeout value in milliseconds, or 'infinity'
        :type timeout: int
        :rtype: :class:`riak.client.index_page.IndexPage`
        """
        if timeout != 'infinity':
            _validate_timeout(timeout)

        page = IndexPage(self, bucket, index, startkey, endkey,
                         return_terms, max_results)

        results, continuation = transport.get_index(
            bucket, index, startkey, endkey, return_terms=return_terms,
            max_results=max_results, continuation=continuation,
            timeout=timeout)

        page.results = results
        page.continuation = continuation
        return page

    def stream_index(self, bucket, index, startkey, endkey=None,
                     return_terms=None, max_results=None, continuation=None,
                     timeout=None):
        """
        Queries a secondary index, streaming matching keys through an
        iterator.

        :param bucket: the bucket whose index will be queried
        :type bucket: RiakBucket
        :param index: the index to query
        :type index: string
        :param startkey: the sole key to query, or beginning of the query range
        :type startkey: string, integer
        :param endkey: the end of the query range (optional if equality)
        :type endkey: string, integer
        :param return_terms: whether to include the secondary index value
        :type return_terms: boolean
        :param max_results: the maximum number of results to return (page size)
        :type max_results: integer
        :param continuation: the opaque continuation returned from a
            previous paginated request
        :type continuation: string
        :param timeout: a timeout value in milliseconds, or 'infinity'
        :type timeout: int
        :rtype: :class:`riak.client.index_page.IndexPage`
        """
        if timeout != 'infinity':
            _validate_timeout(timeout)

        with self._transport() as transport:
            page = IndexPage(self, bucket, index, startkey, endkey,
                             return_terms, max_results)
            page.stream = True
            page.results = transport.stream_index(
                bucket, index, startkey, endkey, return_terms=return_terms,
                max_results=max_results, continuation=continuation,
                timeout=timeout)
            return page

    @retryable
    def get_bucket_props(self, transport, bucket):
        """
        get_bucket_props(bucket)

        Fetches bucket properties for the given bucket.

        .. note:: This request is automatically retried :attr:`RETRY_COUNT`
           times if it fails due to network error.

        :param bucket: the bucket whose properties will be fetched
        :type bucket: RiakBucket
        :rtype: dict
        """
        return transport.get_bucket_props(bucket)

    @retryable
    def set_bucket_props(self, transport, bucket, props):
        """
        set_bucket_props(bucket, props)

        Sets bucket properties for the given bucket.

        .. note:: This request is automatically retried :attr:`RETRY_COUNT`
           times if it fails due to network error.

        :param bucket: the bucket whose properties will be set
        :type bucket: RiakBucket
        :param props: the properties to set
        :type props: dict
        """
        return transport.set_bucket_props(bucket, props)

    @retryable
    def clear_bucket_props(self, transport, bucket):
        """
        clear_bucket_props(bucket)

        Resets bucket properties for the given bucket.

        .. note:: This request is automatically retried :attr:`RETRY_COUNT`
           times if it fails due to network error.

        :param bucket: the bucket whose properties will be set
        :type bucket: RiakBucket
        """
        return transport.clear_bucket_props(bucket)

    @retryable
    def get_keys(self, transport, bucket, timeout=None):
        """
        get_keys(bucket, timeout=None)

        Lists all keys in a bucket.

        .. note:: This request is automatically retried :attr:`RETRY_COUNT`
           times if it fails due to network error.

        :param bucket: the bucket whose properties will be set
        :type bucket: RiakBucket
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :rtype: list
        """
        _validate_timeout(timeout)
        return transport.get_keys(bucket, timeout=timeout)

    def stream_keys(self, bucket, timeout=None):
        """
        Lists all keys in a bucket via a stream. This is a generator
        method which should be iterated over.

        :param bucket: the bucket whose properties will be set
        :type bucket: RiakBucket
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :rtype: iterator
        """
        _validate_timeout(timeout)
        with self._transport() as transport:
            stream = transport.stream_keys(bucket, timeout=timeout)
            try:
                for keylist in stream:
                    if len(keylist) > 0:
                        yield keylist
            finally:
                stream.close()

    @retryable
    def put(self, transport, robj, w=None, dw=None, pw=None, return_body=None,
            if_none_match=None, timeout=None):
        """
        put(robj, w=None, dw=None, pw=None, return_body=None,\
            if_none_match=None, timeout=None)

        Stores an object in the Riak cluster.

        .. note:: This request is automatically retried :attr:`RETRY_COUNT`
           times if it fails due to network error.

        :param robj: the object to store
        :type robj: RiakObject
        :param w: the write quorum
        :type w: integer, string, None
        :param dw: the durable write quorum
        :type dw: integer, string, None
        :param pw: the primary write quorum
        :type pw: integer, string, None
        :param return_body: whether to return the resulting object
           after the write
        :type return_body: boolean
        :param if_none_match: whether to fail the write if the object
          exists
        :type if_none_match: boolean
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        """
        _validate_timeout(timeout)
        return transport.put(robj, w=w, dw=dw, pw=pw,
                             return_body=return_body,
                             if_none_match=if_none_match,
                             timeout=timeout)

    @retryable
    def get(self, transport, robj, r=None, pr=None, timeout=None):
        """
        get(robj, r=None, pr=None, timeout=None)

        Fetches the contents of a Riak object.

        .. note:: This request is automatically retried :attr:`RETRY_COUNT`
           times if it fails due to network error.

        :param robj: the object to fetch
        :type robj: RiakObject
        :param r: the read quorum
        :type r: integer, string, None
        :param pr: the primary read quorum
        :type pr: integer, string, None
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        """
        _validate_timeout(timeout)
        if not isinstance(robj.key, basestring):
            raise TypeError(
                'key must be a string, instead got {0}'.format(repr(robj.key)))

        return transport.get(robj, r=r, pr=pr, timeout=timeout)

    @retryable
    def delete(self, transport, robj, rw=None, r=None, w=None, dw=None,
               pr=None, pw=None, timeout=None):
        """
        delete(robj, rw=None, r=None, w=None, dw=None, pr=None, pw=None,\
               timeout=None)

        Deletes an object from Riak.

        .. note:: This request is automatically retried :attr:`RETRY_COUNT`
           times if it fails due to network error.

        :param robj: the object to store
        :type robj: RiakObject
        :param rw: the read/write (delete) quorum
        :type rw: integer, string, None
        :param r: the read quorum
        :type r: integer, string, None
        :param pr: the primary read quorum
        :type pr: integer, string, None
        :param w: the write quorum
        :type w: integer, string, None
        :param dw: the durable write quorum
        :type dw: integer, string, None
        :param pw: the primary write quorum
        :type pw: integer, string, None
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        """
        _validate_timeout(timeout)
        return transport.delete(robj, rw=rw, r=r, w=w, dw=dw, pr=pr,
                                pw=pw, timeout=timeout)

    @retryable
    def mapred(self, transport, inputs, query, timeout):
        """
        mapred(inputs, query, timeout)

        Executes a MapReduce query.

        .. note:: This request is automatically retried :attr:`RETRY_COUNT`
           times if it fails due to network error.

        :param inputs: the input list/structure
        :type inputs: list, dict
        :param query: the list of query phases
        :type query: list
        :param timeout: the query timeout
        :type timeout: integer, None
        :rtype: mixed
        """
        _validate_timeout(timeout)
        return transport.mapred(inputs, query, timeout)

    def stream_mapred(self, inputs, query, timeout):
        """
        Streams a MapReduce query as (phase, data) pairs. This is a
        generator method which should be iterated over.

        :param inputs: the input list/structure
        :type inputs: list, dict
        :param query: the list of query phases
        :type query: list
        :param timeout: the query timeout
        :type timeout: integer, None
        :rtype: iterator
        """
        _validate_timeout(timeout)
        with self._transport() as transport:
            stream = transport.stream_mapred(inputs, query, timeout)
            try:
                for phase, data in stream:
                    yield phase, data
            finally:
                stream.close()

    @retryable
    def fulltext_search(self, transport, index, query, **params):
        """
        fulltext_search(index, query, **params)

        Performs a full-text search query.

        .. note:: This request is automatically retried :attr:`RETRY_COUNT`
           times if it fails due to network error.

        :param index: the bucket/index to search over
        :type index: string
        :param query: the search query
        :type query: string
        :param params: additional query flags
        :type params: dict
        """
        return transport.search(index, query, **params)

    @retryableHttpOnly
    def fulltext_add(self, transport, index, docs):
        """
        fulltext_add(index, docs)

        Adds documents to the full-text index.

        .. note:: This request is automatically retried
           :attr:`RETRY_COUNT` times if it fails due to network error.
           Only HTTP will be used for this request.

        :param index: the bucket/index in which to index these docs
        :type index: string
        :param docs: the list of documents
        :type docs: list
        """
        transport.fulltext_add(index, docs)

    @retryableHttpOnly
    def fulltext_delete(self, transport, index, docs=None, queries=None):
        """
        fulltext_delete(index, docs=None, queries=None)

        Removes documents from the full-text index.

        .. note:: This request is automatically retried
           :attr:`RETRY_COUNT` times if it fails due to network error.
           Only HTTP will be used for this request.

        :param index: the bucket/index from which to delete
        :type index: string
        :param docs: a list of documents (with ids)
        :type docs: list
        :param queries: a list of queries to match and delete
        :type queries: list
        """
        transport.fulltext_delete(index, docs, queries)

    def multiget(self, pairs, **params):
        """
        Fetches many keys in parallel via threads.

        :param pairs: list of bucket/key tuple pairs
        :type pairs: list
        :param params: additional request flags, e.g. r, pr
        :type params: dict
        :rtype: list of :class:`RiakObject <riak.riak_object.RiakObject>`
            instances
        """
        return multiget(self, pairs, **params)

    @retryable
    def get_counter(self, transport, bucket, key, r=None, pr=None,
                    basic_quorum=None, notfound_ok=None):
        """
        get_counter(bucket, key, r=None, pr=None, basic_quorum=None,\
                    notfound_ok=None)

        Gets the value of a counter.

        .. note:: This request is automatically retried :attr:`RETRY_COUNT`
           times if it fails due to network error.

        :param bucket: the bucket of the counter
        :type bucket: RiakBucket
        :param key: the key of the counter
        :type key: string
        :param r: the read quorum
        :type r: integer, string, None
        :param pr: the primary read quorum
        :type pr: integer, string, None
        :param basic_quorum: whether to use the "basic quorum" policy
           for not-founds
        :type basic_quorum: bool
        :param notfound_ok: whether to treat not-found responses as successful
        :type notfound_ok: bool
        :rtype: integer
        """
        return transport.get_counter(bucket, key, r=r, pr=pr)

    def update_counter(self, bucket, key, value, w=None, dw=None, pw=None,
                       returnvalue=False):
        """
        update_counter(bucket, key, value, w=None, dw=None, pw=None,\
                       returnvalue=False)

        Updates a counter by the given value. This operation is not
        idempotent and so should not be retried automatically.

        :param bucket: the bucket of the counter
        :type bucket: RiakBucket
        :param key: the key of the counter
        :type key: string
        :param value: the amount to increment or decrement
        :type value: integer
        :param w: the write quorum
        :type w: integer, string, None
        :param dw: the durable write quorum
        :type dw: integer, string, None
        :param pw: the primary write quorum
        :type pw: integer, string, None
        :param returnvalue: whether to return the updated value of the counter
        :type returnvalue: bool
        """
        if type(value) not in (int, long):
            raise TypeError("Counter update amount must be an integer")
        if value == 0:
            raise ValueError("Cannot increment counter by 0")

        with self._transport() as transport:
            return transport.update_counter(bucket, key, value,
                                            w=w, dw=dw, pw=pw,
                                            returnvalue=returnvalue)

    increment_counter = update_counter


def _validate_timeout(timeout):
    """
    Raises an exception if the given timeout is an invalid value.
    """
    if not (timeout is None or
            (type(timeout) in (int, long) and
             timeout > 0)):
        raise ValueError("timeout must be a positive integer")
