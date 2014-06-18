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

----------------------------
Query Maintenance Operations
----------------------------

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

--------
Security
--------
All traffic between client and server can be optionally encrypted via OpenSSL.
The server must first be configured to enable security, users and security sources
(see `Managing Security Sources
<http://docs.basho.com/riak/2.0.0/ops/running/security-sources/>`_) must be created
and the correct certificates must be installed.  An overview
can be found at `Authentication and Authorization
<http://docs.basho.com/riak/2.0.0/ops/running/authz/>`_.

.. note:: OpenSSL 1.0.1g or later (or patched version built after 2014-04-01)
          is required for **pyOpenSSL**.

On the client side, connections must then have a
:class:`SecurityCreds<riak.security.SecurityCreds>` object
to specify the required credentials.  The most basic authorization would be
`Trust-based Authentication
<http://docs.basho.com/riak/2.0.0/ops/running/security-sources/#Trust-based-Authentication>`_
which is done exclusively on the server side.

The next level of security would be a username and password.  Simply create a
:class:`SecurityCreds<riak.security.SecurityCreds>` object with a client-side
certificate filename.
That would then need to be passed into the
:class:`RiakClient<riak.client.RiakClient>` initializer.
Optionally the Certification Authority (CA) certificate
may be provided, too::

     creds = SecurityCreds('testuser',
                           'testpass',
                           cacert_file='/path/to/cacert.crt')
     client = RiakClient(credentials=creds)
     myBucket = client.bucket('test')
     val1 = "#SeanCribbsHoldingThings"
     key1 = myBucket.new('hashtag', data=val1)
     key1.store()

If you are using the **protocol buffer** transport you could also add a layer
of security by using certificate-based authentication::

     creds = SecurityCreds('testuser',
                           'testpass',
                           cert_file='/path/to/client.crt',
                           key_file='/path/to/client.key')

.. note:: Username and password are still required for certificate-based
          authentication.

Another security option available is a Certificate Revocation List (CRL).
It lists server certificates which, for whatever reason, are no longer
valid::

     creds = SecurityCreds('testuser',
                           'testpass',
                           cacert_file='/path/to/ca.crt',
                           crl_file='/path/to/server.crl')

The last interesting setting on
:class:`SecurityCreds<riak.security.SecurityCreds>` is the cipher option which
is a colon-delimited list of supported ciphers for encryption::

        creds = SecurityCreds('testuser',
                              'testpass',
                              ciphers='ECDHE-RSA-AES128-SHA256:DHE-RSA-AES256-SHA')

A more detailed
discussion can be found at `Security Ciphers
<http://docs.basho.com/riak/2.0.0/ops/running/authz/#Security-Ciphers>`_.

^^^^^^^^^^^^^^^^^^^^
SecurityCreds object
^^^^^^^^^^^^^^^^^^^^

.. autoclass:: riak.security.SecurityCreds

------------------
Deprecated Methods
------------------

.. warning:: These methods and attributes exist solely for
   backwards-compatibility and should not be used unless code is being
   ported from an older version.

.. automethod:: RiakClient.get_transport
.. automethod:: RiakClient.get_client_id
.. automethod:: RiakClient.set_client_id
.. attribute:: RiakClient.solr

   Returns a RiakSearch object which can access search indexes.

   .. deprecated:: 2.0.0
      Use the ``fulltext_*`` methods instead.

.. automethod:: RiakClient.get_r
.. automethod:: RiakClient.set_r
.. automethod:: RiakClient.get_pr
.. automethod:: RiakClient.set_pr
.. automethod:: RiakClient.get_w
.. automethod:: RiakClient.set_w
.. automethod:: RiakClient.get_dw
.. automethod:: RiakClient.set_dw
.. automethod:: RiakClient.get_pw
.. automethod:: RiakClient.set_pw
.. automethod:: RiakClient.get_rw
.. automethod:: RiakClient.set_rw
