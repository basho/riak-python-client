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

-----------------------
Bucket-level Operations
-----------------------

.. automethod:: RiakClient.get_bucket_type_props
.. automethod:: RiakClient.set_bucket_type_props
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
.. automethod:: RiakClient.get_counter
.. automethod:: RiakClient.update_counter

----------------
Query Operations
----------------

.. automethod:: RiakClient.mapred
.. automethod:: RiakClient.stream_mapred
.. automethod:: RiakClient.get_index
.. automethod:: RiakClient.stream_index
.. automethod:: RiakClient.fulltext_search
.. automethod:: RiakClient.fulltext_add
.. automethod:: RiakClient.fulltext_delete

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
