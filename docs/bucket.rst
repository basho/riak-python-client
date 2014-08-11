.. _bucket_types:

======================
Buckets & Bucket Types
======================

.. currentmodule:: riak.bucket

**Buckets** are both namespaces for the key-value pairs you store in Riak,
and containers for properties that apply to that namespace.  In older
versions of Riak, this was the only logical organization available.
Now a higher-level collection called a **Bucket Type** can group buckets
together.  They also allows for efficiently setting properties on a
group of buckets at the same time.

Unlike buckets, Bucket Types must be `explicitly created
<http://docs.basho.com/riak/2.0.0/dev/advanced/bucket-types/#Managing-Bucket-Types-Through-the-Command-Line>`_
and activated before being used::

   riak-admin bucket-type create n_equals_1 '{"props":{"n_val":1}}'
   riak-admin bucket-type activate n_equals_1

Note that these two properties may not be modified after creation:
``consistent`` and ``datatype``. Other properties may be updated. Buckets
grouped under a Bucket Type inherit all of the Type's properties. Each
bucket may override individual properties but some properties cannot be
overridden.

Bucket Type administration is only supported via the ``riak-admin bucket-type``
command interface. This release does not include an API to perform these
actions.  However, the Python client has been updated to set and retrieve
bucket properties for a bucket under a given Bucket Type.

If Bucket Types are not specifed, the *default* bucket
type is used.  These buckets should be created via the :meth:`bucket()
<riak.client.RiakClient.bucket>` method on the client object, like so::

    import riak

    client = riak.RiakClient()
    mybucket = client.bucket('mybucket')

Buckets with a user-specified Bucket Type can also be created via the same
:meth:`bucket()<riak.client.RiakClient.bucket>` method with
an additional parameter or explicitly via
:meth:`bucket_type()<riak.client.RiakClient.bucket_type>`::

    import riak

    client = riak.RiakClient()
    mybucket = client.bucket('mybucket','mybuckettype')
    othertype = client.bucket_type('othertype')
    otherbucket = othertype.bucket('otherbucket')

For more detailed discussion see `Using Bucket Types
<http://docs.basho.com/riak/2.0.0/dev/advanced/bucket-types/>`_.

--------------
Bucket objects
--------------

.. autoclass:: RiakBucket

   .. attribute:: name

      The name of the bucket, a string.

   .. autoattribute:: resolver

-----------------
Bucket properties
-----------------

Bucket properties are flags and defaults that apply to all keys in the
bucket.

.. automethod:: RiakBucket.get_properties
.. automethod:: RiakBucket.set_properties
.. automethod:: RiakBucket.clear_properties
.. automethod:: RiakBucket.get_property
.. automethod:: RiakBucket.set_property

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Shortcuts for common properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Some of the most commonly-used bucket properties are exposed as object
properties as well. The getters and setters simply call
:meth:`RiakBucket.get_property` and :meth:`RiakBucket.set_property`
respectively.

.. autoattribute:: RiakBucket.n_val
.. autoattribute:: RiakBucket.allow_mult
.. autoattribute:: RiakBucket.r
.. autoattribute:: RiakBucket.pr
.. autoattribute:: RiakBucket.w
.. autoattribute:: RiakBucket.dw
.. autoattribute:: RiakBucket.pw
.. autoattribute:: RiakBucket.rw

^^^^^^^^^^^^^^^^^^^^
Shortcuts for search
^^^^^^^^^^^^^^^^^^^^

When Riak Search is enabled on the server, you can toggle which
buckets have automatic indexing turned on using the ``search`` bucket
property (and on older versions, the ``precommit`` property). These
methods simplify interacting with that configuration.

.. automethod:: RiakBucket.search_enabled
.. automethod:: RiakBucket.enable_search
.. automethod:: RiakBucket.disable_search

-----------------
Working with keys
-----------------

The primary purpose of buckets is to act as namespaces for keys. As
such, you can use the bucket object to create, fetch and delete
:class:`objects <riak.riak_object.RiakObject>`.

.. automethod:: RiakBucket.new
.. automethod:: RiakBucket.new_from_file
.. automethod:: RiakBucket.get
.. automethod:: RiakBucket.multiget
.. automethod:: RiakBucket.delete


----------------
Query operations
----------------

.. automethod:: RiakBucket.search
.. automethod:: RiakBucket.get_index
.. automethod:: RiakBucket.stream_index


-------------
Serialization
-------------

Similar to :class:`RiakClient <riak.client.RiakClient>`, buckets can
register custom transformation functions for media-types. When
undefined on the bucket, :meth:`RiakBucket.get_encoder` and
:meth:`RiakBucket.get_decoder` will delegate to the client associated
with the bucket.

.. automethod:: RiakBucket.get_encoder
.. automethod:: RiakBucket.set_encoder
.. automethod:: RiakBucket.get_decoder
.. automethod:: RiakBucket.set_decoder

------------
Listing keys
------------

Shortcuts for :meth:`RiakClient.get_keys()
<riak.client.RiakClient.get_keys>` and
:meth:`RiakClient.stream_keys()
<riak.client.RiakClient.stream_keys>` are exposed on the bucket
object. The same admonitions for these operations apply.

.. automethod:: RiakBucket.get_keys
.. automethod:: RiakBucket.stream_keys

-------------------
Bucket Type objects
-------------------

.. autoclass:: BucketType

   .. attribute:: name

      The name of the Bucket Type, a string.

----------------------
Bucket Type properties
----------------------

Bucket Type properties are flags and defaults that apply to all buckets in the
Bucket Type.

.. automethod:: BucketType.is_default
.. automethod:: BucketType.get_properties
.. automethod:: BucketType.set_properties
.. automethod:: BucketType.get_property
.. automethod:: BucketType.set_property
.. method:: BucketType.datatype

   The assigned datatype for this bucket type, if present.

   :rtype: None or str
 
---------------
Listing buckets
---------------

Shortcuts for :meth:`RiakClient.get_buckets()
<riak.client.RiakClient.get_buckets>` and
:meth:`RiakClient.stream_buckets()
<riak.client.RiakClient.stream_buckets>` are exposed on the bucket
type object.  This is similar to `Listing keys`_ on buckets.

.. automethod:: BucketType.bucket
.. automethod:: BucketType.get_buckets
.. automethod:: BucketType.stream_buckets


