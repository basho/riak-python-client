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


class RiakClientOperations(RiakClientTransport):
    """
    Methods for RiakClient that result in requests sent to the Riak
    cluster.

    Note that all of these methods have an implicit 'transport'
    argument that will be prepended automatically as part of the retry
    logic, and does not need to be supplied by the user.
    """

    @retryable
    def get_buckets(self, transport):
        """
        Get the list of buckets as RiakBucket instances.
        NOTE: Do not use this in production, as it requires traversing through
        all keys stored in a cluster.
        """
        return [self.bucket(name) for name in transport.get_buckets()]

    @retryable
    def ping(self, transport):
        """
        Check if the Riak server for this ``RiakClient`` instance is alive.

        :rtype: boolean
        """
        return transport.ping()

    is_alive = ping

    @retryable
    def get_index(self, transport, bucket, index, startkey, endkey=None):
        """
        Queries a secondary index, returning matching keys.

        :param bucket: the bucket whose index will be queried
        :type bucket: RiakBucket
        :param index: the index to query
        :type index: string
        :param startkey: the sole key to query, or beginning of the query range
        :type startkey: string, integer
        :param endkey: the end of the query range (optional if equality)
        :type endkey: string, integer
        :rtype: list
        """
        return transport.get_index(bucket, index, startkey, endkey)

    @retryable
    def get_bucket_props(self, transport, bucket):
        """
        Fetches bucket properties for the given bucket.

        :param bucket: the bucket whose properties will be fetched
        :type bucket: RiakBucket
        :rtype: dict
        """
        return transport.get_bucket_props(bucket)

    @retryable
    def set_bucket_props(self, transport, bucket, props):
        """
        Sets bucket properties for the given bucket.

        :param bucket: the bucket whose properties will be set
        :type bucket: RiakBucket
        :param props: the properties to set
        :type props: dict
        """
        return transport.set_bucket_props(bucket, props)

    @retryable
    def clear_bucket_props(self, transport, bucket):
        """
        Resets bucket properties for the given bucket.

        :param bucket: the bucket whose properties will be set
        :type bucket: RiakBucket
        """
        return transport.clear_bucket_props(bucket)

    @retryable
    def get_keys(self, transport, bucket):
        """
        Lists all keys in a bucket.

        :param bucket: the bucket whose properties will be set
        :type bucket: RiakBucket
        :rtype: list
        """
        return transport.get_keys(bucket)

    def stream_keys(self, bucket):
        """
        Lists all keys in a bucket via a stream. This is a generator
        method which should be iterated over.


        :param bucket: the bucket whose properties will be set
        :type bucket: RiakBucket
        :rtype: iterator
        """
        with self._transport() as transport:
            stream = transport.stream_keys(bucket)
            try:
                for keylist in stream:
                    if len(keylist) > 0:
                        yield keylist
            finally:
                stream.close()

    @retryable
    def put(self, transport, robj, w=None, dw=None, pw=None, return_body=None,
            if_none_match=None):
        """
        Stores an object in the Riak cluster.

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
        """
        return transport.put(robj, w=w, dw=dw, pw=pw,
                             return_body=return_body,
                             if_none_match=if_none_match)

    @retryable
    def put_new(self, transport, robj, w=None, dw=None, pw=None,
                return_body=None, if_none_match=None):
        """
        Stores an object in the Riak cluster with a generated key.

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
        """
        return transport.put_new(robj, w=w, dw=dw, pw=pw,
                                 return_body=return_body,
                                 if_none_match=if_none_match)

    @retryable
    def get(self, transport, robj, r=None, pr=None, vtag=None):
        """
        Fetches the contents of a Riak object.

        :param robj: the object to fetch
        :type robj: RiakObject
        :param r: the read quorum
        :type r: integer, string, None
        :param pr: the primary read quorum
        :type pr: integer, string, None
        :param vtag: the specific sibling to fetch
        :type vtag: string
        """
        return transport.get(robj, r=r, pr=pr, vtag=vtag)

    @retryable
    def delete(self, transport, robj, rw=None, r=None, w=None, dw=None,
               pr=None, pw=None):
        """
        Deletes an object from Riak.

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
        """
        return transport.delete(robj, rw=rw, r=r, w=w, dw=dw, pr=pr,
                                pw=pw)

    @retryable
    def mapred(self, transport, inputs, query, timeout):
        """
        Executes a MapReduce query.

        :param inputs: the input list/structure
        :type inputs: list, dict
        :param query: the list of query phases
        :type query: list
        :param timeout: the query timeout
        :type timeout: integer, None
        :rtype: mixed
        """
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
        with self._transport() as transport:
            stream = transport.stream_mapred(inputs, query, timeout)
            try:
                for phase, data in stream:
                    yield phase, data
            finally:
                stream.close()

    @retryableHttpOnly
    def fulltext_search(self, transport, index, query, **params):
        """
        Performs a full-text search query.

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
        Adds documents to the full-text index.

        :param index: the bucket/index in which to index these docs
        :type index: string
        :param docs: the list of documents
        :type docs: list
        """
        transport.fulltext_add(index, docs)

    @retryableHttpOnly
    def fulltext_delete(self, transport, index, docs=None, queries=None):
        """
        Removes documents from the full-text index.

        :param index: the bucket/index from which to delete
        :type index: string
        :param docs: a list of documents (with ids)
        :type docs: list
        :param queries: a list of queries to match and delete
        :type queries: list
        """
        transport.fulltext_delete(index, docs, queries)
