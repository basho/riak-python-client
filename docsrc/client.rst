====================
Client & Connections
====================

To connect to a Riak cluster, you must create a
:py:class:`~riak.client.RiakClient` object. The default configuration
connects to a single Riak node on ``localhost`` with the default
ports. The below instantiation statements are all equivalent::

    from riak import RiakClient, RiakNode

    RiakClient()
    RiakClient(protocol='http', host='127.0.0.1', http_port=8098)
    RiakClient(nodes=[{'host':'127.0.0.1','http_port':8098}])
    RiakClient(protocol='http', nodes=[RiakNode()])


.. note:: Connections are not established until you attempt to perform
   an operation. If the host or port are incorrect, you will not get
   an error raised immediately.

The client maintains a connection pool behind the scenes, one for each
protocol. Connections are opened as-needed; a random node is selected
when a new connection is requested.

--------------
Client objects
--------------

.. currentmodule:: riak.client
.. autoclass:: RiakClient

   .. autoattribute:: PROTOCOLS

      Prior to Riak 2.0 the ``'https'`` protocol was also an option, but now
      secure connections are handled by the :ref:`security-label` feature.

   .. autoattribute:: protocol
   .. autoattribute:: client_id
   .. autoattribute:: resolver
   .. attribute:: nodes

      The list of :class:`nodes <riak.node.RiakNode>` that this
      client will connect to. It is best not to modify this property
      directly, as it is not thread-safe.

^^^^^
Nodes
^^^^^

The :attr:`nodes <RiakClient.nodes>` attribute of ``RiakClient`` objects is
a list of ``RiakNode`` objects. If you include multiple host
specifications in the ``RiakClient`` constructor, they will be turned
into this type.

.. autoclass:: riak.node.RiakNode
   :members:

^^^^^^^^^^^
Retry logic
^^^^^^^^^^^

Some operations that fail because of network errors or Riak node
failure may be safely retried on another node, and the client will do
so automatically. The items below can be used to configure this
behavior.

.. autoattribute:: RiakClient.retries

.. automethod:: RiakClient.retry_count

.. autodata:: riak.client.transport.DEFAULT_RETRY_COUNT

-----------------------
Client-level Operations
-----------------------

Some operations are not scoped by buckets or bucket types and can be
performed on the client directly:

.. automethod:: RiakClient.ping
.. automethod:: RiakClient.get_buckets
.. automethod:: RiakClient.stream_buckets

----------------------------------
Accessing Bucket Types and Buckets
----------------------------------

Most client operations are on :py:class:`bucket type objects
<riak.bucket.BucketType>`, the :py:class:`bucket objects
<riak.bucket.RiakBucket>` they contain or keys within those buckets. Use the
``bucket_type`` or ``bucket`` methods for creating bucket types and buckets
that will proxy operations to the called client.

.. automethod:: RiakClient.bucket_type
.. automethod:: RiakClient.bucket

----------------------
Bucket Type Operations
----------------------

.. automethod:: RiakClient.get_bucket_type_props
.. automethod:: RiakClient.set_bucket_type_props

-----------------
Bucket Operations
-----------------

.. automethod:: RiakClient.get_bucket_props
.. automethod:: RiakClient.set_bucket_props
.. automethod:: RiakClient.clear_bucket_props
.. automethod:: RiakClient.get_keys
.. automethod:: RiakClient.stream_keys

--------------------
Key-level Operations
--------------------

.. automethod:: RiakClient.get
.. automethod:: RiakClient.put
.. automethod:: RiakClient.delete
.. automethod:: RiakClient.multiget
.. automethod:: RiakClient.fetch_datatype
.. automethod:: RiakClient.update_datatype

--------------------
Timeseries Operations
--------------------

.. automethod:: RiakClient.ts_describe
.. automethod:: RiakClient.ts_get
.. automethod:: RiakClient.ts_put
.. automethod:: RiakClient.ts_delete
.. automethod:: RiakClient.ts_query
.. automethod:: RiakClient.ts_stream_keys

----------------
Query Operations
----------------

.. automethod:: RiakClient.mapred
.. automethod:: RiakClient.stream_mapred
.. automethod:: RiakClient.get_index
.. automethod:: RiakClient.stream_index
.. automethod:: RiakClient.fulltext_search
.. automethod:: RiakClient.paginate_index
.. automethod:: RiakClient.paginate_stream_index

-----------------------------
Search Maintenance Operations
-----------------------------

.. automethod:: RiakClient.create_search_schema
.. automethod:: RiakClient.get_search_schema
.. automethod:: RiakClient.create_search_index
.. automethod:: RiakClient.get_search_index
.. automethod:: RiakClient.delete_search_index
.. automethod:: RiakClient.list_search_indexes

-------------
Serialization
-------------

The client supports automatic transformation of Riak responses into
Python types if encoders and decoders are registered for the
media-types. Supported by default are ``application/json`` and
``text/plain``.

.. autofunction:: default_encoder
.. automethod:: RiakClient.get_encoder
.. automethod:: RiakClient.set_encoder
.. automethod:: RiakClient.get_decoder
.. automethod:: RiakClient.set_decoder

-------------------
Deprecated Features
-------------------

^^^^^^^^^^^^^^^^
Full-text search
^^^^^^^^^^^^^^^^

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

.. automethod:: RiakClient.fulltext_add
.. automethod:: RiakClient.fulltext_delete

.. _legacy_counters:

^^^^^^^^^^^^^^^
Legacy Counters
^^^^^^^^^^^^^^^

The first Data Type introduced in Riak 1.4 were `counters`.  These pre-date
:ref:`Bucket Types <bucket_types>` and the current implementation.
Rather than returning objects, the counter operations
act directly on the value of the counter.  Legacy counters are deprecated
as of Riak 2.0.  Please use :py:class:`~riak.datatypes.Counter` instead.

.. warning:: Legacy counters are incompatible with Bucket Types.

.. automethod:: RiakClient.get_counter
.. automethod:: RiakClient.update_counter
