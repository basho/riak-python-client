========================
Python Client for Riak
========================

Documentation
=============

`Documentation for the Riak Python Client Library <http://basho.github.io/riak-python-client/index.html>`_ is available here.
The documentation source is found in `docs/ subdirectory
<https://github.com/basho/riak-python-client/tree/master/docs>`_ and can be
built with `Sphinx <http://sphinx.pocoo.org/>`_.

Documentation for Riak is available at http://docs.basho.com/riak/latest

Install
=======

The recommended version of Python for use with this client is Python 2.7.

You must have `Protocol Buffers`_ installed before you can install the Riak Client. From the Riak Python Client root directory, execute::

    python setup.py install

There is an additional dependency on the Python package `setuptools`.  Please install `setuptools` first, e.g. ``port install py27-setuptools`` for OS X and MacPorts.

Unit Test
=========
To run the unit tests against a Riak server (with default TCP port configuration) on localhost, execute::

    python setup.py test

If you don't have `Riak Search <http://wiki.basho.com/Riak-Search.html>`_ enabled, you can set the ``SKIP_SEARCH`` environment variable to 1 skip those tests.

If you don't have `Yokozuna <https://github.com/basho/yokozuna>`_  enabled, you can set the ``RUN_YZ`` environment variable to 0 to skip those tests.

If your Riak server isn't running on localhost or you have built a Riak devrel from source, use the environment variables ``RIAK_TEST_HOST``, ``RIAK_TEST_HTTP_PORT`` and  ``RIAK_TEST_PB_PORT=8087`` to specify where to find the Riak server.

Some of the connection tests need port numbers that are NOT in use.
If ports 1023 and 1022 are in use on your test system, set the environment variables ``DUMMY_HTTP_PORT`` and ``DUMMY_PB_PORT`` to unused port numbers.

