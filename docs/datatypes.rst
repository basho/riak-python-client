==========
Data Types
==========

Before Riak 1.4 all data stored was an opaque binary type.  Then
came the introduction of :py:class:`Counter
<riak.datatypes.Counter>`, the first data type supported
in Riak.  In Riak 2.0, several other data types were introduced
creating this full list:

    * :py:class:`Counter <riak.datatypes.Counter>`
    * :py:class:`Flags <riak.datatypes.Flag>`
    * :py:class:`Maps <riak.datatypes.Map>`
    * :py:class:`Registers <riak.datatypes.Register>`
    * :py:class:`Sets <riak.datatypes.Set>`

An in-depth discussion of data types, also known as CRDTs,
can be found at `Data Types
<http://docs.basho.com/riak/2.0.0/theory/concepts/crdts/>`_.

Examples of using data types can be found at
`Using Data Types
<http://docs.basho.com/riak/2.0.0/dev/using/data-types/>`_.

------------------------
Data type abstract class
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
Data type classes
-----------------

.. autoclass:: Counter

.. autoattribute:: Counter.modified
.. automethod:: Counter.to_op
.. automethod:: Counter.increment
.. automethod:: Counter.decrement

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

.. autoclass:: Set

.. autoattribute:: Set.modified
.. automethod:: Set.to_op
.. automethod:: Set.add
.. automethod:: Set.discard

^^^^^^^^^^^^^^^^
Map-only objects
^^^^^^^^^^^^^^^^

A couple of the new datatypes must be embedded in
:py:class:`Maps <riak.datatypes.Map>` objects (in addition to
:py:class:`Map <riak.datatypes.Map>` itself):

.. autoclass:: Register

.. autoattribute:: Register.value
.. autoattribute:: Register.modified
.. automethod:: Register.to_op
.. automethod:: Register.assign

.. autoclass:: Flag

.. autoattribute:: Flag.modified
.. automethod:: Flag.to_op
.. automethod:: Flag.enable
.. automethod:: Flag.disable
