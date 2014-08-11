.. _datatypes:

==========
Data Types
==========

Traditionally all data stored in Riak was an opaque binary type.  Then
in version 1.4 came the introduction of a :ref:`counter <legacy_counters>`,
the first Convergent Data Type supported
in Riak.  In Riak 2.0, several additional Data Types were introduced.  Riak
"knows" about these data types, and conflicting
writes to them will converge automatically without presenting sibling values
to the user.

Here is the list of current Data Types:

    * :py:class:`~riak.datatypes.Counter` increments or decrements
      integer values
    * :py:class:`~riak.datatypes.Set` allows you to store multiple
      distinct opaque binary values against a key
    * :py:class:`~riak.datatypes.Map` is a nested, recursive
      struct, or associative array. Think of it as a container for
      composing ad hoc data structures from multiple Data Types.
      Inside a map you may store sets, counters, flags,
      registers, and even other maps
    * :py:class:`~riak.datatypes.Register` stores binaries
      accoring to last-write-wins logic within
      :py:class:`~riak.datatypes.Map`
    * :py:class:`~riak.datatypes.Flag` is similar to a boolean
      and also must be within :py:class:`~riak.datatypes.Map`

All Data Types must be stored in buckets bearing a :py:class:`Bucket Type
<riak.bucket.BucketType>` that sets
the datatype property to one of ``counter``, ``set``, or ``map``. Note that
the bucket must have the ``allow_mult`` property set to ``true``.

These Data Types are wrapped in a regular `riak_object`, so size constraints
that apply to normal Riak values apply to Riak Data Types too.

An in-depth discussion of Data Types, also known as CRDTs,
can be found at `Data Types
<http://docs.basho.com/riak/2.0.0/theory/concepts/crdts/>`_.

Examples of using Data Types can be found at
`Using Data Types
<http://docs.basho.com/riak/2.0.0/dev/using/data-types/>`_.

--------------------
Data Type operations
--------------------

Riak Data Types provide a further departure from Riak's usual operation,
in that the API is operation-based. Rather than fetching the data structure,
reconciling conflicts, mutating the result, and writing it back, you instead
tell Riak what operations to perform on the Data Type. Here are some example
operations:

   * increment counter by 10
   * add 'joe' to set
   * remove the Set field called 'friends' from the Map
   * set the prepay flag to true in the Map

-----------------
Data Type context
-----------------

In order for Riak Data Types to behave well, you must return the opaque context
received from a read when you:

   * Set a :py:class:`~riak.datatypes.Flag` to ``false``
   * Remove a field from a :py:class:`~riak.datatypes.Map`
   * Remove an element from a :py:class:`~riak.datatypes.Set`

The basic rule is "you cannot remove something you haven't seen", and the
context tells Riak what you've actually seen. The Python client handles
opaque contexts for you.

------------------------
Data Type abstract class
------------------------

.. currentmodule:: riak.datatypes
.. autoclass:: Datatype

.. autoattribute:: Datatype.value
.. autoattribute:: Datatype.context
.. autoattribute:: Datatype.modified
.. automethod:: Datatype.reload
.. automethod:: Datatype.update
.. automethod:: Datatype.clear
.. automethod:: Datatype.to_op

-----------------
Data Type classes
-----------------

^^^^^^^^^^^^^
Counter class
^^^^^^^^^^^^^

.. autoclass:: Counter

.. autoattribute:: Counter.modified
.. automethod:: Counter.to_op
.. automethod:: Counter.increment
.. automethod:: Counter.decrement

^^^^^^^^^
Map class
^^^^^^^^^

.. autoclass:: Map

.. autoattribute:: Map.value
.. autoattribute:: Map.modified
.. automethod:: Map.reload
.. automethod:: Map.update
.. automethod:: Map.clear
.. automethod:: Map.to_op
.. autoattribute:: Map.counters
.. autoattribute:: Map.flags
.. autoattribute:: Map.maps
.. autoattribute:: Map.registers
.. autoattribute:: Map.sets

^^^^^^^^^
Set class
^^^^^^^^^

.. autoclass:: Set

.. autoattribute:: Set.modified
.. automethod:: Set.to_op
.. automethod:: Set.add
.. automethod:: Set.discard

^^^^^^^^^^^^^^^^
Map-only objects
^^^^^^^^^^^^^^^^

Two of the new Data Types may only be embedded in
:py:class:`Maps <riak.datatypes.Map>` objects (in addition to
:py:class:`Map <riak.datatypes.Map>` itself):

++++++++++++++
Register class
++++++++++++++

.. autoclass:: Register

.. autoattribute:: Register.value
.. autoattribute:: Register.modified
.. automethod:: Register.to_op
.. automethod:: Register.assign

++++++++++
Flag class
++++++++++

.. autoclass:: Flag

.. autoattribute:: Flag.modified
.. automethod:: Flag.to_op
.. automethod:: Flag.enable
.. automethod:: Flag.disable

.. _legacy_counters:

---------------
Legacy Counters
---------------

The first Data Type introduced in Riak 1.4 were `counters`.  These pre-date
:ref:`Bucket Types <bucket_types>` and the current implementation.
Rather than returning objects, the counter operations
act directly on the value of the counter.

.. warning:: Legacy counters are deprecated as of Riak 2.0.  Please use
             :py:class:`~riak.datatypes.Counter` instead.  They are also
             incompatible with Bucket Types.

.. automethod:: riak.bucket.RiakBucket.get_counter
.. automethod:: riak.bucket.RiakBucket.update_counter
