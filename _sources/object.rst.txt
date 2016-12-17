================
Values & Objects
================

.. currentmodule:: riak.riak_object

Keys in Riak are namespaced into :class:`buckets
<riak.bucket.RiakBucket>`, and their associated values are represented
by :class:`objects <RiakObject>`, not to be confused with Python
"objects". A :class:`RiakObject` is a container for the key, the
:ref:`vclock`, the value(s) and any metadata associated with the
value(s).

Values may also be :class:`datatypes <riak.datatypes.Datatype>`, but
are not discussed here.

----------
RiakObject
----------

.. autoclass:: RiakObject

   .. attribute:: key

      The key of this object, a string. If not present, the server
      will generate a key the first time this object is stored.

   .. attribute:: bucket

      The :class:`bucket <riak.bucket.RiakBucket>` to which this
      object belongs.

   .. autoattribute:: resolver
   .. attribute:: vclock

      The :ref:`vclock` for this object.

   .. autoattribute:: exists

.. _vclock:

^^^^^^^^^^^^
Vector clock
^^^^^^^^^^^^

Vector clocks are Riak's means of tracking the relationships between
writes to a key. It is best practice to fetch the latest version of a
key before attempting to modify or overwrite the value; if you do not,
you may create :ref:`siblings` or lose data! The content of a vector
clock is essentially opaque to the user.

.. autoclass:: VClock

-----------
Persistence
-----------

Fetching, storing, and deleting keys are the bread-and-butter of Riak.

.. automethod:: RiakObject.store
.. automethod:: RiakObject.reload
.. automethod:: RiakObject.delete

.. _object_accessors:

------------------
Value and Metadata
------------------

Unless you have enabled :ref:`siblings` via the :attr:`allow_mult
<riak.bucket.RiakBucket.allow_mult>` bucket property, you can
inspect and manipulate the value and metadata of an object directly using these
properties and methods:

.. autoattribute:: RiakObject.data
.. autoattribute:: RiakObject.encoded_data
.. autoattribute:: RiakObject.content_type
.. autoattribute:: RiakObject.charset
.. autoattribute:: RiakObject.content_encoding
.. autoattribute:: RiakObject.last_modified
.. autoattribute:: RiakObject.etag
.. autoattribute:: RiakObject.usermeta
.. autoattribute:: RiakObject.links
.. autoattribute:: RiakObject.indexes
.. automethod:: RiakObject.add_index
.. automethod:: RiakObject.remove_index
.. automethod:: RiakObject.set_index
.. automethod:: RiakObject.add_link

.. _siblings: 

--------
Siblings
--------

Because Riak's consistency model is "eventual" (and not linearizable),
there is no way for it to disambiguate writes that happen
concurrently. The :ref:`vclock` helps establish a
"happens after" relationships so that concurrent writes can be
detected, but with the exception of :ref:`datatypes`, Riak has no way
to determine which write has the correct value. 

Instead, when :attr:`allow_mult <riak.bucket.RiakBucket.allow_mult>`
is ``True``, Riak keeps all writes that appear to be concurrent. Thus,
the contents of a key's value may, in fact, be multiple values, which
are called "siblings". Siblings are modeled in :class:`RiakContent
<riak.content.RiakContent>` objects, which contain all of the same
:ref:`object_accessors` methods and attributes as the parent object.

.. autoattribute:: RiakObject.siblings

.. autoclass:: riak.content.RiakContent

You do not typically have to create :class:`RiakContent
<riak.content.RiakContent>` objects yourself, but they will be created
for you when :meth:`fetching <RiakObject.reload>` objects from Riak.

.. note:: The :ref:`object_accessors` accessors on :class:`RiakObject`
   are actually proxied to the first sibling when the object has only
   one.


^^^^^^^^^^^^^^^^^^^^^^^
Conflicts and Resolvers
^^^^^^^^^^^^^^^^^^^^^^^

When an object is *not* in conflict, it has only one sibling. When it
is in conflict, you will have to resolve the conflict before it can be
written again. How you choose to resolve the conflict is up to you,
but you can automate the process using a :attr:`resolver
<RiakObject.resolver>` function.

.. autofunction:: riak.resolver.default_resolver
.. autofunction:: riak.resolver.last_written_resolver

If you do not supply a resolver function, or your resolver leaves
multiple siblings present, accessing the :ref:`object_accessors` will
result in a :exc:`ConflictError <riak.ConflictError>` being raised.

.. autoexception:: riak.ConflictError
