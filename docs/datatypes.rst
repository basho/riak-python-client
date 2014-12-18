.. _datatypes:
.. currentmodule:: riak.datatypes

==========
Data Types
==========

Traditionally all data stored in Riak was an opaque binary type. Then
in version 1.4 came the introduction of a :ref:`counter
<legacy_counters>`, the first Convergent Data Type supported in Riak.
In Riak 2.0, several additional Data Types were introduced. Riak
"knows" about these data types, and conflicting writes to them will
converge automatically without presenting :ref:`sibling values
<siblings>` to the user.

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

All Data Types must be stored in buckets bearing a
:class:`~riak.bucket.BucketType` that sets the
:attr:`~riak.bucket.BucketType.datatype` property to one of
``"counter"``, ``"set"``, or ``"map"``. Note that the bucket must have
the ``allow_mult`` property set to ``true``.

These Data Types are stored just like :class:`RiakObjects
<riak.riak_object.RiakObject>`, so size constraints that apply to
normal Riak values apply to Riak Data Types too.

An in-depth discussion of Data Types, also known as CRDTs,
can be found at `Data Types
<http://docs.basho.com/riak/2.0.0/theory/concepts/crdts/>`_.

Examples of using Data Types can be found at
`Using Data Types
<http://docs.basho.com/riak/2.0.0/dev/using/data-types/>`_.

------------------
Sending Operations
------------------

Riak Data Types provide a further departure from Riak's usual operation,
in that the API is operation-based. Rather than fetching the data structure,
reconciling conflicts, mutating the result, and writing it back, you instead
tell Riak what operations to perform on the Data Type. Here are some example
operations:

   * increment a :class:`Counter` by ``10``
   * add ``'joe'`` to a :class:`Set`
   * remove the :class:`Set` field called ``'friends'`` from a :class:`Map`
   * enable the prepay :class:`Flag` in a :class:`Map`

Datatypes can be fetched and created just like
:class:`~riak.riak_object.RiakObject` instances, using
:meth:`RiakBucket.get <riak.bucket.RiakBucket.get>` and
:meth:`RiakBucket.new <riak.bucket.RiakBucket.new>`, except that the
bucket must belong to a bucket-type that has a valid datatype
property. If we have a bucket-type named "social-graph" that has the
datatype `"set"`, we would fetch a :class:`Set` like so::

    graph = client.bucket_type('social-graph')
    graph.datatype  # => 'set'
    myfollowers = graph.bucket('followers').get('seancribbs')
    # => a Set datatype

Once we have a datatype, we can stage operations against it and then
send those operations to Riak::

   myfollowers.add('javajolt')
   myfollowers.discard('roach')
   myfollowers.update()

While this looks in code very similar to manipulating
:class:`~riak.riak_object.RiakObject` instances, only mutations are
enqueued locally, not the new value.

---------------------------
Context and Observed-Remove
---------------------------

In order for Riak Data Types to behave well, you must have an opaque
context received from a read when you:

   * :meth:`disable <Flag.disable>` a :class:`Flag`
     (set it to ``false``)
   * remove a field from a :class:`Map`
   * :meth:`remove <Set.discard>` an element from a :py:class:`Set`

The basic rule is "you cannot remove something you haven't seen", and
the context tells Riak what you've actually seen, similar to the
:ref:`vclock` on :class:`~riak.riak_object.RiakObject`. The Python
client handles opaque contexts for you transparently as long as you
fetch before performing one of these actions.

------------------------
Datatype abstract class
------------------------

.. autoclass:: Datatype

   .. autoattribute:: value
   .. autoattribute:: context
   .. autoattribute:: modified

^^^^^^^^^^^^^^^^^^^
Persistence methods
^^^^^^^^^^^^^^^^^^^

.. automethod:: Datatype.reload
.. automethod:: Datatype.update
.. function:: Datatype.store(**params)

    This is an alias for :meth:`~riak.datatypes.Datatype.update`.

.. automethod:: Datatype.delete
.. automethod:: Datatype.clear

-------
Counter
-------

.. autoclass:: Counter

.. attribute:: Counter.value

   The current value of the counter.

   :rtype: int

.. automethod:: Counter.increment
.. automethod:: Counter.decrement

---
Set
---

.. autoclass:: Set

.. attribute:: Set.value

   An immutable copy of the current value of the set.

   :rtype: frozenset

.. automethod:: Set.add
.. automethod:: Set.discard

---
Map
---

.. autoclass:: Map

.. autoattribute:: Map.value

.. attribute:: Map.counters

   Filters keys in the map to only those of counter types. Example::

        map.counters['views'].increment()
        del map.counters['points']


.. attribute:: Map.flags

   Filters keys in the map to only those of flag types. Example::

        map.flags['confirmed'].enable()
        del map.flags['attending']


.. attribute:: Map.maps

   Filters keys in the map to only those of map types. Example::

        map.maps['emails'].registers['home'].set("user@example.com")
        del map.maps['spam']


.. attribute:: Map.registers

   Filters keys in the map to only those of register types. Example::

        map.registers['username'].set_value("riak-user")
        del map.registers['access_key']

.. attribute:: Map.sets

   Filters keys in the map to only those of set types. Example::

        map.sets['friends'].add("brett")
        del map.sets['favorites']

------------------
Map-only datatypes
------------------

Two of the new Data Types may only be embedded in
:py:class:`Map <riak.datatypes.Map>` objects (in addition to
:py:class:`Map <riak.datatypes.Map>` itself):

--------
Register
--------

.. autoclass:: Register

.. autoattribute:: Register.value
.. automethod:: Register.assign

----
Flag
----

.. autoclass:: Flag

.. attribute:: Flag.value

   The current value of the flag.

   :rtype: bool, None

.. automethod:: Flag.enable
.. automethod:: Flag.disable
