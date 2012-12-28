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

from riak.bucket import RiakBucket
from transport import RiakClientTransport


class RiakClientOperations(RiakClientTransport):
    """
    Methods for RiakClient that result in requests sent to the Riak
    cluster.
    """

    def get_buckets(self):
        """
        Get the list of buckets as RiakBucket instances.
        NOTE: Do not use this in production, as it requires traversing through
        all keys stored in a cluster.
        """
        with self._transport() as transport:
            return [RiakBucket(self, name) for name in transport.get_buckets()]

    def ping(self):
        """
        Check if the Riak server for this ``RiakClient`` instance is alive.

        :rtype: boolean
        """
        with self._transport() as transport:
            return transport.ping()

    is_alive = ping

    def get_index(self, bucket, index, startkey, endkey=None):
        """
        Queries a secondary index, returning matching keys.
        """
        with self._transport() as transport:
            return transport.get_index(bucket, index, startkey, endkey)

    def get_bucket_props(self, bucket):
        """
        Fetches bucket properties for the given bucket.
        """
        with self._transport() as transport:
            return transport.get_bucket_props(bucket)

    def set_bucket_props(self, bucket, props):
        """
        Sets bucket properties for the given bucket.
        """
        with self._transport() as transport:
            return transport.set_bucket_props(bucket, props)

    def get_keys(self, bucket):
        """
        Lists all keys in a bucket.
        """
        with self._transport() as transport:
            return transport.get_keys(bucket)

    def stream_keys(self, bucket):
        """
        Lists all keys in a bucket via a stream. This is a generator
        method which should be iterated over.
        """
        with self._transport() as transport:
            for keylist in transport.stream_keys(bucket):
                yield keylist

    def put(self, robj, w=None, dw=None, pw=None, return_body=None,
            if_none_match=None):
        """
        Stores an object in the Riak cluster.
        """
        with self._transport() as transport:
            return transport.put(robj, w=w, dw=dw, pw=pw,
                                 return_body=return_body,
                                 if_none_match=if_none_match)

    def put_new(self, robj, w=None, dw=None, pw=None, return_body=None,
                if_none_match=None):
        """
        Stores an object in the Riak cluster with a generated key.
        """
        with self._transport() as transport:
            return transport.put_new(robj, w=w, dw=dw, pw=pw,
                                     return_body=return_body,
                                     if_none_match=if_none_match)

    def get(self, robj, r=None, pr=None, vtag=None):
        """
        Fetches the contents of a Riak object.
        """
        with self._transport() as transport:
            return transport.get(robj, r=r, pr=pr, vtag=vtag)

    def delete(self, robj, rw=None, r=None, w=None, dw=None, pr=None, pw=None):
        """
        Deletes an object from Riak.
        """
        with self._transport() as transport:
            return transport.delete(robj, rw=rw, r=r, w=w, dw=dw, pr=pr,
                                    pw=pw)

    def mapred(self, inputs, query, timeout):
        """
        Executes a MapReduce query
        """
        with self._transport() as transport:
            return transport.mapred(inputs, query, timeout)

    def stream_mapred(self, inputs, query, timeout):
        """
        Streams a MapReduce query as (phase, data) pairs. This is a
        generator method which should be iterated over.
        """
        with self._transport() as transport:
            for phase, data in transport.stream_mapred(inputs, query, timeout):
                yield phase, data

    def fulltext_search(self, index, query, **params):
        """
        Performs a full-text search query.
        """
        with self._transport() as transport:
            return transport.search(index, query, **params)

    def fulltext_add(self, index, docs):
        """
        Adds documents to the full-text index.
        """
        with self._transport(protocol='http') as transport:
            transport.fulltext_add(index, docs)

    def fulltext_delete(self, index, docs=None, queries=None):
        """
        Removes documents from the full-text index.
        """
        with self._transport(protocol='http') as transport:
            transport.fulltext_delete(index, docs, queries)
