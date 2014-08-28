Deprecated Features
===================

.. _riak_search_1_0:

---------------
Riak Search 1.0
---------------

The original version of Riak Search has been replaced by :ref:`yz-label`,
which is full-blown Solr integration with Riak.

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


.. _legacy_counters:

---------------
Legacy Counters
---------------

The first Data Type introduced in Riak 1.4 were `counters`.  These pre-date
:ref:`Bucket Types <bucket_types>` and the current implementation.
Rather than returning objects, the counter operations
act directly on the value of the counter.  Legacy counters are deprecated
as of Riak 2.0.  Please use :py:class:`~riak.datatypes.Counter` instead.  

.. warning:: Legacy counters are incompatible with Bucket Types.

.. automethod:: riak.bucket.RiakBucket.get_counter
.. automethod:: riak.bucket.RiakBucket.update_counter

----------
RiakBucket
----------

These methods have been removed from :class:`~riak.bucket.RiakBucket` as of
Riak 2.0:

    * ``get_binary()`` replaced by :meth:`~riak.bucket.RiakBucket.get`
    * ``new_binary()`` replaced by :meth:`~riak.bucket.RiakBucket.new`
    * ``new_binary_from_file()`` replaced by
      :meth:`~riak.bucket.RiakBucket.new_from_file`
    * ``get_r()``
    * ``set_r()``
    * ``get_pr()``
    * ``set_pr()``
    * ``get_w()``
    * ``set_w()``
    * ``get_dw()``
    * ``set_dw()``
    * ``get_pw()``
    * ``set_pw()``
    * ``get_rw()``
    * ``set_rw()``

The quorum accessors (r, pr, w, etc.) were removed as well. Please use
bucket property or request option instead.

----------
RiakClient
----------

These methods have also been removed from :class:`riak.client.RiakClient`
in the Riak 2.0 release:

    * ``get_client_id()`` replaced by :attr:`~riak.client.RiakClient.client_id`
    * ``set_client_id()`` replaced by :attr:`~riak.client.RiakClient.client_id`
    * ``solr``
    * ``get_transport()``
    * ``get_r()``
    * ``set_r()``
    * ``get_pr()``
    * ``set_pr()``
    * ``get_w()``
    * ``set_w()``
    * ``get_dw()``
    * ``set_dw()``
    * ``get_pw()``
    * ``set_pw()``
    * ``get_rw()``
    * ``set_rw()``

With the addition of `vector clocks
<http://docs.basho.com/riak/2.0.0/theory/concepts/Vector-Clocks>`_
in Riak, client IDs are no longer useful or necessary. The ``solr`` interface
has been replaced by fully integrated Solr, aka :ref:`yz-label`.  See
:ref:`riak_search_1_0`

The quorum accessors (r, pr, w, etc.) were removed as well. Please use
bucket property or request option instead.

The optional `port` argument to the :class:`~riak.client.RiakClient` initializer
has been replaced with `pb_port` and `http_port`.

----------
RiakObject
----------

The method ``get_sibling`` was replaced by the
:attr:`~riak.RiakObject.siblings` attribute in Riak 2.0.

The siblings in :class:`riak.RiakObject` are modeled in the
:class:`riak.RiakContent` content which had two methods deprecated in the
Riak 2.0 release:

    * ``get_encoded_data()`` replaced by :attr:`~riak.RiakObject.encoded_data`
    * ``set_encoded_data()`` replaced by :attr:`~riak.RiakObject.encoded_data`
