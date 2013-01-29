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
        """
        return transport.get_index(bucket, index, startkey, endkey)

    @retryable
    def get_bucket_props(self, transport, bucket):
        """
        Fetches bucket properties for the given bucket.
        """
        return transport.get_bucket_props(bucket)

    @retryable
    def set_bucket_props(self, transport, bucket, props):
        """
        Sets bucket properties for the given bucket.
        """
        return transport.set_bucket_props(bucket, props)

    @retryable
    def get_keys(self, transport, bucket):
        """
        Lists all keys in a bucket.
        """
        return transport.get_keys(bucket)

    @retryable
    def stream_keys(self, transport, bucket):
        """
        Lists all keys in a bucket via a stream. This is a generator
        method which should be iterated over.
        """
        for keylist in transport.stream_keys(bucket):
            if len(keylist) > 0:
                yield keylist

    @retryable
    def put(self, transport, robj, w=None, dw=None, pw=None, return_body=None,
            if_none_match=None):
        """
        Stores an object in the Riak cluster.
        """
        return transport.put(robj, w=w, dw=dw, pw=pw,
                             return_body=return_body,
                             if_none_match=if_none_match)

    @retryable
    def put_new(self, transport, robj, w=None, dw=None, pw=None, return_body=None,
                if_none_match=None):
        """
        Stores an object in the Riak cluster with a generated key.
        """
        return transport.put_new(robj, w=w, dw=dw, pw=pw,
                                 return_body=return_body,
                                 if_none_match=if_none_match)

    @retryable
    def get(self, transport, robj, r=None, pr=None, vtag=None):
        """
        Fetches the contents of a Riak object.
        """
        return transport.get(robj, r=r, pr=pr, vtag=vtag)

    @retryable
    def delete(self, transport, robj, rw=None, r=None, w=None, dw=None, pr=None,
               pw=None):
        """
        Deletes an object from Riak.
        """
        return transport.delete(robj, rw=rw, r=r, w=w, dw=dw, pr=pr,
                                pw=pw)

    @retryable
    def mapred(self, transport, inputs, query, timeout):
        """
        Executes a MapReduce query
        """
        return transport.mapred(inputs, query, timeout)

    @retryable
    def stream_mapred(self, transport, inputs, query, timeout):
        """
        Streams a MapReduce query as (phase, data) pairs. This is a
        generator method which should be iterated over.
        """
        for phase, data in transport.stream_mapred(inputs, query, timeout):
            yield phase, data

    @retryableHttpOnly
    def fulltext_search(self, transport, index, query, **params):
        """
        Performs a full-text search query.
        """
        return transport.search(index, query, **params)

    @retryableHttpOnly
    def fulltext_add(self, transport, index, docs):
        """
        Adds documents to the full-text index.
        """
        transport.fulltext_add(index, docs)

    @retryableHttpOnly
    def fulltext_delete(self, transport, index, docs=None, queries=None):
        """
        Removes documents from the full-text index.
        """
        transport.fulltext_delete(index, docs, queries)
