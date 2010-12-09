.. ref-client:

==========
RiakClient
==========

The ``RiakClient`` object holds information necessary to connect to Riak. The
Riak API uses HTTP, so there is no persistent connection, and the ``RiakClient``
object is extremely lightweight.


RiakClient Methods
==================

``__init__``
~~~~~~~~~~~~

.. method:: RiakClient.__init__(self, host='127.0.0.1', port=8098, prefix='riak', mapred_prefix='mapred', transport_class=None, client_id=None)

Constructs a new ``RiakClient`` object.


``get_transport``
-----------------

.. method:: RiakClient.get_transport(self)

Gets the transport instance the client is using for it's connection.


``get_r``
---------

.. method:: RiakClient.get_r(self)

Gets the R-value setting for this ``RiakClient`` instance. Default is "quorum"
value as an integer.


``set_r``
---------

.. method:: RiakClient.set_r(self, r)

Set the R-value for this RiakClient. This value will be used
for any calls to ``get`` or ``get_binary`` where where no
R-value is specified in the method call and no R-value has
been set in the RiakBucket.

Accepts an integer for the ``r`` value. Returns the ``RiakClient`` instance.


``get_w``
---------

.. method:: RiakClient.get_w(self)

Gets the W-value setting for this ``RiakClient`` instance. Default is "quorum"
value as an integer.


``set_w``
---------

.. method:: RiakClient.set_w(self, w)

Sets the W-value for this ``RiakClient`` instance. See ``RiakClient.set_r`` for
a description of how these values are used.

Accepts an integer for the ``w`` value. Returns the ``RiakClient`` instance.


``get_dw``
----------

.. method:: RiakClient.get_dw(self)

Gets the DW-value for this ``RiakClient`` instance. Default is the "quorum"
value as an integer.


``set_dw``
----------

.. method:: RiakClient.set_dw(self, dw)

Sets the DW-value for this ``RiakClient`` instance. See ``RiakClient.set_r`` for
a description of how these values are used.

Accepts an integer for the ``dw`` value. Returns the ``RiakClient`` instance.


``get_rw``
----------

.. method:: RiakClient.get_rw(self)

Gets the RW-value for this ``RiakClient`` instance. Default is the "quorum"
value as an integer.


``set_rw``
----------

.. method:: RiakClient.set_rw(self, rw)

Sets the RW-value for this ``RiakClient`` instance. See ``RiakClient.set_r``
for a description of how these values are used.

Accepts an integer for the ``rw`` value. Returns the ``RiakClient`` instance.


``get_client_id``
-----------------

.. method:: RiakClient.get_client_id(self)

Gets the ``client_id`` for this ``RiakClient`` instance.


``set_client_id``
-----------------

.. method:: RiakClient.set_client_id(self, client_id)

Sets the ``client_id`` for this ``RiakClient`` instance. The ``client_id``
should be a string.

.. warning::

  You should not call this method unless you know what you are doing.


``get_encoder``
---------------

.. method:: RiakClient.get_encoder(self, content_type)

Gets the encoding function for the provided content type.


``set_encoder``
---------------

.. method:: RiakClient.set_encoder(self, content_type, encoder)

Sets the encoding function for the provided content type. The ``encoder``
argument should be a callable.


``get_decoder``
---------------

.. method:: RiakClient.get_decoder(self, content_type)

Gets the decoding function for the provided content type.


``set_decoder``
---------------

.. method:: RiakClient.set_decoder(self, content_type, decoder)

Sets the decoding function for the provided content type. The ``decoder``
argument should be a callable.


``bucket``
----------

.. method:: RiakClient.bucket(self, name)

Gets the bucket by the specified ``name``. Since buckets always exist,
this will always return a ``RiakBucket`` instance.


``is_alive``
------------

.. method:: RiakClient.is_alive(self)

Checks to see if the Riak server for this ``RiakClient`` is alive. Returns
``True`` if alive, ``False`` otherwise.


``add``
-------

.. method:: RiakClient.add(self, *args)

Start assembling a Map/Reduce operation. A shortcut for ``RiakMapReduce.add``.

Returns a ``RiakMapReduce`` instance.


``search``
----------

.. method:: RiakClient.search(self, *args)

Start assembling a Map/Reduce operation based on search results. This command
will return an error unless executed against a Riak Search cluster. A shortcut
for ``RiakMapReduce.search``.

Returns a ``RiakMapReduce`` instance.


``link``
--------

.. method:: RiakClient.link(self, *args)

Start assembling a Map/Reduce operation involving links. A shortcut for
``RiakMapReduce.link``.

Returns a ``RiakMapReduce`` instance.


``map``
-------

.. method:: RiakClient.map(self, *args)

Start assembling a Map/Reduce operation. A shortcut for ``RiakMapReduce.map``.

Returns a ``RiakMapReduce`` instance.


``reduce``
----------

.. method:: RiakClient.reduce(self, *args)

Start assembling a Map/Reduce operation. A shortcut for
``RiakMapReduce.reduce``.

Returns a ``RiakMapReduce`` instance.
