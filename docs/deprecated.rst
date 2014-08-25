Deprecated Features
===================

---------------
Fulltext Search
---------------

The original version of Riak Search has been replaced by :ref:`yz-label`,
which is full blown Solr integration with Riak.

If Riak Search 1.0 is enabled, you can query an index via the bucket's
:meth:`~riak.bucket.RiakBucket.search` method::

    bucket.enable_search()
    bucket.new("one", data={'value':'one'},
               content_type="application/json").store()

    bucket.search('value=one')

To manually add and remove documents from an index (without an
associated key), use the :class:`~riak.client.RiakClient`
:meth:`~riak.client.RiakClient.fulltext_add` and
:meth:`~riak.client.RiakClient.fulltext_delete` methods directly.

