.. ref-bucket:

==========
RiakBucket
==========

The ``RiakBucket`` object allows you to access and change information about a
Riak bucket, and provides methods to create or retrieve objects within the bucket.


RiakBucket Methods
==================

``__init__``
------------

.. method:: RiakBucket.__init__(self, client, name)

Returns a new ``RiakBucket`` instance. The ``client`` argument should be a
``RiakClient`` instance and the ``name`` argument should be the bucket name as
a string.


``get_name``
------------

.. method:: RiakBucket.get_name(self)

Gets the bucket name as a string.


``get_r``
---------

.. method:: RiakBucket.get_r(self, r=None)

Gets the R-value for this bucket, if it is set. If not set, it will return
the R-value for the client as an integer.


``set_r``
---------

.. method:: RiakBucket.set_r(self, r)

Sets the R-value for this bucket. ``RiakBucket.get`` and
``RiakBucket.get_binary`` operations that do not specify an R-value will use
this value.

Accepts an integer for the ``r`` value. Returns the ``RiakBucket`` instance.


``get_w``
---------

.. method:: RiakBucket.get_w(self, w=None)

Gets the W-value for this bucket, if it is set. If not set, it will return
the W-value for the client as an integer.


``set_w``
---------

.. method:: RiakBucket.set_w(self, w)

Sets the W-value for this bucket. See ``RiakBucket.set_r`` for more information.

Accepts an integer for the ``w`` value. Returns the ``RiakBucket`` instance.


``get_dw``
----------

.. method:: RiakBucket.get_dw(self, dw=None)

Gets the DW-value for this bucket, if it is set. If not set, it will return
the DW-value for the client as an integer.


``set_dw``
----------

.. method:: RiakBucket.set_dw(self, dw)

Sets the DW-value for this bucket. See ``RiakBucket.set_r`` for more
information.

Accepts an integer for the ``dw`` value. Returns the ``RiakBucket`` instance.


``get_rw``
----------

.. method:: RiakBucket.get_rw(self, rw=None)

Gets the RW-value for this bucket, if it is set. If not set, it will return
the RW-value for the client as an integer.


``set_rw``
----------

.. method:: RiakBucket.set_rw(self, rw)

Sets the RW-value for this bucket. See ``RiakBucket.set_r`` for more
information.

Accepts an integer for the ``rw`` value. Returns the ``RiakBucket`` instance.


``get_encoder``
---------------

.. method:: RiakBucket.get_encoder(self, content_type)

Gets the encoding function for the provided content type for this bucket.


``set_encoder``
---------------

.. method:: RiakBucket.set_encoder(self, content_type, encoder)

Sets the encoding function for the provided content type for this bucket.
The ``encoder`` argument should be a callable.


``get_decoder``
---------------

.. method:: RiakBucket.get_decoder(self, content_type)

Gets the decoding function for the provided content type for this bucket.


``set_decoder``
---------------

.. method:: RiakBucket.set_decoder(self, content_type, decoder)

Sets the decoding function for the provided content type for this bucket. The
``decoder`` argument should be a callable.


``new``
-------

.. method:: RiakBucket.new(self, key, data=None, content_type='application/json')

Creates a new ``RiakObject`` that will be stored as JSON. A shortcut for
manually instantiating a ``RiakObject``. The ``key`` argument should be a
string.

The ``data`` should be a JSON-encodable structure (usually a Python dictionary).

Returns the ``RiakObject`` instance.


``new_binary``
--------------

.. method:: RiakBucket.new_binary(self, key, data, content_type='application/octet-stream')

Create a new Riak object that will be stored as plain text/binary.

Creates a new ``RiakObject`` that will be stored as plain text/binary. A
shortcut for manually instantiating a ``RiakObject``. The ``key`` argument
should be a string.

The ``data`` should be a plain text/binary string.

The ``content_type`` argument should be the MIME type of the content. The
default is ``application/octet-stream``.

Returns the ``RiakObject`` instance.


``get``
-------

.. method:: RiakBucket.get(self, key, r=None)

Retrieve a JSON-encoded object from Riak. The ``key`` should be a string.

The optional ``r`` is the R-value of the request. Defaults to the bucket's
R-value if not provided.

Returns the ``RiakObject`` instance.


``get_binary``
--------------

.. method:: RiakBucket.get_binary(self, key, r=None)

Retrieve a binary object/string from Riak. The ``key`` should be a string.

The optional ``r`` is the R-value of the request. Defaults to the bucket's
R-value if not provided.

Returns the ``RiakObject`` instance.


``set_n_val``
-------------

.. method:: RiakBucket.set_n_val(self, nval)

Sets the N-value for this bucket, which is the number of replicas that will be
written of each object in the bucket. Accepts the ``nval`` as an integer.

.. warning::

  Set this once before you write any data to the bucket and never change it
  again. Unpredictable things could happen if you subsequently change it. This
  method should only be used if you know what you are doing.


``get_n_val``
-------------

.. method:: RiakBucket.get_n_val(self)

Retrieves the N-value as an integer for this bucket.


``set_default_r_val``
---------------------

.. method:: RiakBucket.set_default_r_val(self, rval)


``get_default_r_val``
---------------------

.. method:: RiakBucket.get_default_r_val(self)


``set_default_w_val``
---------------------

.. method:: RiakBucket.set_default_w_val(self, wval)


``get_default_w_val``
---------------------

.. method:: RiakBucket.get_default_w_val(self)


``set_default_dw_val``
----------------------

.. method:: RiakBucket.set_default_dw_val(self, dwval)


``get_default_dw_val``
----------------------

.. method:: RiakBucket.get_default_dw_val(self)


``set_default_rw_val``
----------------------

.. method:: RiakBucket.set_default_rw_val(self, rwval)


``get_default_rw_val``
----------------------

.. method:: RiakBucket.get_default_rw_val(self)


``set_allow_multiples``
-----------------------

.. method:: RiakBucket.set_allow_multiples(self, bool)

If set to ``True``, writes with conflicting data will be stored and returned
to the client. This situation can be detected by calling
``RiakObject.has_siblings`` and ``RiakObject.get_siblings``.

.. warning::

  This should only be used if you know what you are doing, as it can lead to
  unexpected results.


``get_allow_multiples``
-----------------------

.. method:: RiakBucket.get_allow_multiples(self)

Retrieves the ``allow multiples`` setting. Returns ``True`` or ``False``.


``set_property``
----------------

.. method:: RiakBucket.set_property(self, key, value)

Sets a property on the bucket. The ``key`` should be the property name as a
string. The ``value`` can be of mixed type.

.. warning::

  This should only be used if you know what you are doing.


``get_bool_property``
---------------------

.. method:: RiakBucket.get_bool_property(self, key)

Gets a boolean property on the bucket. Converts to a ``True`` or ``False``
value.


``get_property``
----------------

.. method:: RiakBucket.get_property(self, key)

Retrieves a property on the bucket. The return value will be of mixed type.


``set_properties``
------------------

.. method:: RiakBucket.set_properties(self, props)

Sets multiple properties on the bucket in one call. The ``props`` argument
should be a dictionary of property names/values to be set.

.. warning::

  This should only be used if you know what you are doing.


``get_properties``
------------------

.. method:: RiakBucket.get_properties(self)

Retrieve a dictionary of all bucket properties.


``get_keys``
------------

.. method:: RiakBucket.get_keys(self)

Returns all keys within the bucket.

.. warning::

  At current, this is a very expensive operation. Use with caution.
