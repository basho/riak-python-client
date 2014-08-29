==========================
Advanced Usage & Internals
==========================

This page contains documentation for aspects of library internals that
you will rarely need to interact with, but are important for
understanding how it works and development purposes.

---------------
Connection pool
---------------

.. currentmodule:: riak.transports.pool

.. autoexception:: BadResource
.. autoclass:: Resource
   :members:
.. autoclass:: Pool
   :members:

.. autoclass:: PoolIterator

-----------
Retry logic
-----------

.. currentmodule:: riak.client.transport

.. autoclass:: RiakClientTransport
   :members:
   :private-members:

.. autofunction:: _is_retryable

.. autofunction:: retryable

.. autofunction:: retryableHttpOnly

--------
Multiget
--------

.. currentmodule:: riak.client.multiget

.. autodata:: POOL_SIZE

.. autoclass:: Task

.. autoclass:: MultiGetPool
   :members:
   :private-members:

.. autodata:: RIAK_MULTIGET_POOL

.. autofunction:: multiget

---------
Datatypes
---------

.. currentmodule:: riak.datatypes

.. autoclass:: riak.datatypes.map.TypedMapView
   :members:
   :special-members:

.. autodata:: TYPES

----------
Transports
----------

.. currentmodule:: riak.transports.transport

.. autoclass:: RiakTransport
   :members:
   :private-members:

.. currentmodule:: riak.transports.feature_detect

.. autoclass:: FeatureDetection
   :members:
   :private-members:

^^^^^^^^^^^^^^^^
Security helpers
^^^^^^^^^^^^^^^^

.. currentmodule:: riak.transports.security

.. autofunction:: verify_cb
.. autofunction:: configure_context

.. autoclass:: RiakWrappedSocket
.. autoclass:: fileobject

^^^^^^^^^^^^^^
HTTP Transport
^^^^^^^^^^^^^^

.. currentmodule:: riak.transports.http

.. autoclass:: RiakHttpPool

.. autofunction:: is_retryable

.. autoclass:: RiakHttpTransport
   :members:

^^^^^^^^^^^^^^^^^^^^^^^^^^
Protocol Buffers Transport
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. currentmodule:: riak.transports.pbc

.. autoclass:: RiakPbcTransport
   :members:

---------
Utilities
---------

^^^^^^^^^^^^^^^^^^
Link wrapper class
^^^^^^^^^^^^^^^^^^

.. autoclass:: riak.mapreduce.RiakLink

^^^^^^^^^^^^^^^^^
Multi-valued Dict
^^^^^^^^^^^^^^^^^

.. currentmodule:: riak.multidict

.. autoclass:: MultiDict

   .. automethod:: add
   .. automethod:: getall
   .. automethod:: getone
   .. automethod:: mixed
   .. automethod:: dict_of_lists

^^^^^^^^^^^^^^^^^^
Micro-benchmarking
^^^^^^^^^^^^^^^^^^

.. currentmodule:: riak.benchmark

.. autofunction:: measure

.. autofunction:: measure_with_rehearsal

.. autoclass:: Benchmark
   :members:

^^^^^^^^^^^^^
Miscellaneous
^^^^^^^^^^^^^

.. currentmodule:: riak.util

.. autofunction:: quacks_like_dict

.. autofunction:: deep_merge

.. autofunction:: deprecated

.. autoclass:: lazy_property

------------------
distutils commands
------------------

.. automodule:: commands
   :members:
   :undoc-members:
