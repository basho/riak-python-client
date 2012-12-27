from riak.transports import RiakHttpTransport
from xml.etree import ElementTree


class RiakSearch(object):
    def __init__(self, client, **unused_args):
        self._client = client

    def add(self, index, *docs):
        self._client.fulltext_add(index, docs)

    index = add

    def delete(self, index, docs=None, queries=None):
        self._client.fulltext_delete(index, docs, queries)

    remove = delete

    def search(self, index, query, **params):
        return self._client.fulltext_search(index, query, **params)

    select = search
