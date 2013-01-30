"""
Copyright 2010 Basho Technologies, Inc.

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


class RiakSearch(object):
    """
    A wrapper around Riak Search-related client operations. See
    :func:`RiakClient.solr`.
    """

    def __init__(self, client, **unused_args):
        self._client = client

    def add(self, index, *docs):
        """
        Adds documents to a fulltext index. Shortcut and backwards
        compatibility for :func:`RiakClientOperations.fulltext_add`.
        """
        self._client.fulltext_add(index, docs=docs)

    index = add

    def delete(self, index, docs=None, queries=None):
        """
        Removes documents from a fulltext index. Shortcut and backwards
        compatibility for :func:`RiakClientOperations.fulltext_delete`.
        """
        self._client.fulltext_delete(index, docs=docs, queries=queries)

    remove = delete

    def search(self, index, query, **params):
        """
        Searches a fulltext index. Shortcut and backwards
        compatibility for :func:`RiakClientOperations.fulltext_search`.
        """
        return self._client.fulltext_search(index, query, **params)

    select = search
