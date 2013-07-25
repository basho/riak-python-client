=======
Buckets
=======

.. currentmodule:: riak.bucket

--------
Overview
--------

Buckets are both namespaces for the key-value pairs you store in Riak,
and containers for properties that apply to that namespace. Buckets
should be created via the :meth:`bucket()
<riak.client.RiakClient.bucket>` method on the client object, like so::

    import riak

    client = riak.RiakClient()
    mybucket = client.bucket('mybucket')

--------------
RiakBucket API
--------------

.. autoclass:: RiakBucket

   .. attribute:: name

      The name of the bucket, a string.

   .. autoattribute:: resolver

^^^^^^^^^^^^^^^^^
Bucket properties
^^^^^^^^^^^^^^^^^

Bucket properties are flags and defaults that apply to all keys in the
bucket.

.. automethod:: RiakBucket.get_properties
.. automethod:: RiakBucket.set_properties
.. automethod:: RiakBucket.clear_properties
.. automethod:: RiakBucket.get_property
.. automethod:: RiakBucket.set_property

"""""""""""""""""""""""""""""""
Shortcuts for common properties
"""""""""""""""""""""""""""""""

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

""""""""""""""""""""
Shortcuts for search
""""""""""""""""""""

When Riak Search is enabled on the server, you can toggle which
buckets have automatic indexing turned on using the ``search`` bucket
property (and on older versions, the ``precommit`` property). These
methods simplify interacting with that configuration.

.. automethod:: RiakBucket.search_enabled
.. automethod:: RiakBucket.enable_search
.. automethod:: RiakBucket.disable_search

^^^^^^^^^^^^^^^^^
Working with keys
^^^^^^^^^^^^^^^^^

The primary purpose of buckets is to act as namespaces for keys. As
such, you can use the bucket object to create, fetch and delete
:class:`objects <riak.riak_object.RiakObject>`.

.. automethod:: RiakBucket.new
.. automethod:: RiakBucket.new_from_file
.. automethod:: RiakBucket.get
.. automethod:: RiakBucket.multiget
.. automethod:: RiakBucket.delete

""""""""
Counters
""""""""

Rather than returning objects, the counter operations new to Riak 1.4
act directly on the value of the counter.

.. automethod:: RiakBucket.get_counter
.. automethod:: RiakBucket.update_counter

^^^^^^^^^^^^^^^^
Query operations
^^^^^^^^^^^^^^^^

.. automethod:: RiakBucket.search
.. automethod:: RiakBucket.get_index
.. automethod:: RiakBucket.stream_index


^^^^^^^^^^^^^
Serialization
^^^^^^^^^^^^^

Similar to :class:`RiakClient <riak.client.RiakClient>`, buckets can
register custom transformation functions for media-types. When
undefined on the bucket, :meth:`RiakBucket.get_encoder` and
:meth:`RiakBucket.get_decoder` will delegate to the client associated
with the bucket.

.. automethod:: RiakBucket.get_encoder
.. automethod:: RiakBucket.set_encoder
.. automethod:: RiakBucket.get_decoder
.. automethod:: RiakBucket.set_decoder


^^^^^^^^^^^^
Listing keys
^^^^^^^^^^^^

Shortcuts for :meth:`RiakClient.get_keys()
<riak.client.RiakClient.get_keys>` and
:meth:`RiakClient.stream_keys()
<riak.client.RiakClient.stream_keys>` are exposed on the bucket
object. The same admonitions for these operations apply.

.. automethod:: RiakBucket.get_keys
.. automethod:: RiakBucket.stream_keys

^^^^^^^^^^^^^^^^^^
Deprecated methods
^^^^^^^^^^^^^^^^^^

.. warning:: These methods exist solely for backwards-compatibility and should not
   be used unless code is being ported from an older version.

.. automethod:: RiakBucket.new_binary
.. automethod:: RiakBucket.new_binary_from_file
.. automethod:: RiakBucket.get_binary
.. automethod:: RiakBucket.get_r
.. automethod:: RiakBucket.set_r
.. automethod:: RiakBucket.get_pr
.. automethod:: RiakBucket.set_pr
.. automethod:: RiakBucket.get_w
.. automethod:: RiakBucket.set_w
.. automethod:: RiakBucket.get_dw
.. automethod:: RiakBucket.set_dw
.. automethod:: RiakBucket.get_pw
.. automethod:: RiakBucket.set_pw
.. automethod:: RiakBucket.get_rw
.. automethod:: RiakBucket.set_rw
