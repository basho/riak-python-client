======================
Python Client for Riak
======================

Documentation
=============

`Documentation for the Riak Python Client Library
<http://basho.github.io/riak-python-client/index.html>`_ is available
here. The documentation source is found in `docs/ subdirectory
<https://github.com/basho/riak-python-client/tree/master/docs>`_ and
can be built with `Sphinx <http://sphinx.pocoo.org/>`_.

Documentation for Riak is available at http://docs.basho.com/riak/latest

Install
=======

The recommended versions of Python for use with this client are Python
`2.7.x` and `3.3/3.4`.

From the Riak Python Client root directory, execute

From Source
-----------

.. code-block:: console

    python setup.py install

There is an additional dependency on the Python package `setuptools`.

From PyPI
---------

Official packages are signed and published to `PyPI
<https://pypi.python.org/pypi/riak>`_.

To install from `PyPI <https://pypi.python.org/pypi/riak>`_ directly you can use
`pip`.  

.. code-block:: console
    
    pip install riak


Testing
=======

To setup the default test configuration build a test Riak node (from
a ``riak`` directory)

.. code-block:: console

   make rel

See `Basic Cluster Setup
<http://docs.basho.com/riak/2.0.0/ops/building/basic-cluster-setup/>`_
for more details.

For all of the simple default values, set the ``RIAK_DIR`` environment
variable to the root of your Riak installation.  Then from the
``riak-python-client`` directory 

.. code-block:: console

   cd buildbot
   make preconfigure

Start your Riak node with ``riak start`` from the the Riak directory,
then back in ``buildbot`` type

.. code-block:: console

   make configure
   make test

That will run the test suite twice: once with security enabled and once
without.

Testing Options
---------------

If you wish to change the default options you can run the setup by hand.
First configure the test node by adjusting the ``riak.conf``
settings, where ``RIAK_DIR`` is the path to the top your
Riak installation

.. code-block:: console

   python setup.py preconfigure --riak-conf=$RIAK_DIR/etc/riak.conf

Optionally the hostname and port numbers can be changed, too, via these
arguments:

    - ``--host=`` IP of host running Riak (default is ``localhost``)
    - ``--pb-port=`` protocol buffers port number (default is ``8087``)
    - ``--http-port=`` http port number (default is ``8098``)
    - ``--https-port=`` https port number (default is ``8099``)

You may alternately add these lines to ``setup.cfg``

.. code-block:: ini

    [preconfigure]
    riak-conf=/Users/sean/dev/riak/rel/riak/etc/riak.conf
    host=localhost
    pb-port=8087
    http-port=8098
    https-port=8099

Next start the test node.  Once it is running, a test configuration is
installed which includes security test users and bucket types

.. code-block:: console

    python setup.py configure --riak-admin=$RIAK_DIR/bin/riak-admin

Optionally these configuration settings can be changed, too:

   - ``--username=`` test user account (default is ``testuser``)
   - ``--password=`` password for test user account (default is
     ``testpassword``)
   - ``--certuser=`` secruity test user account (default is ``certuser``)
   - ``--certpass=`` password for security test user account (default is
     ``certpass``)

Similarly ``setup.cfg`` may be modified instead.  To run the tests against a
Riak server (with configured TCP port configuration) on localhost, execute

.. code-block:: console

    python setup.py test

Connections to Riak in Tests
----------------------------

If your Riak server isn't running on localhost or you have built a
Riak devrel from source, use the environment variables
``RIAK_TEST_HOST``, ``RIAK_TEST_HTTP_PORT`` and
``RIAK_TEST_PB_PORT`` to specify where to find the Riak server.
``RIAK_TEST_PROTOCOL`` to specify which protocol to test.  Can be
either ``pbc`` or ``http``.

Some of the connection tests need port numbers that are NOT in use. If
ports 1023 and 1022 are in use on your test system, set the
environment variables ``DUMMY_HTTP_PORT`` and ``DUMMY_PB_PORT`` to
unused port numbers.

Testing Search
--------------

If you don't have `Riak Search
<http://docs.basho.com/riak/latest/dev/using/search/>`_ enabled, you
can set the ``RUN_SEARCH`` environment variable to 0 skip those
tests.

If you don't have `Search 2.0 <https://github.com/basho/yokozuna>`_
enabled, you can set the ``RUN_YZ`` environment variable to 0 to skip
those tests.

Testing Bucket Types (Riak 2+)
------------------------------

To test bucket-types, you must run the ``create_bucket_types`` setup
command, which will create the bucket-types used in testing, or create
them manually yourself. It can be run like so (substituting ``$RIAK``
with the root of your Riak install)

.. code-block:: console

    ./setup.py create_bucket_types --riak-admin=$RIAK/bin/riak-admin

You may alternately add these lines to `setup.cfg`

.. code-block:: ini

    [create_bucket_types]
    riak-admin=/Users/sean/dev/riak/rel/riak/bin/riak-admin

To skip the bucket-type tests, set the ``RUN_BTYPES`` environment
variable to ``0``.

Testing Data Types (Riak 2+)
----------------------------

To test data types, you must set up bucket types (see above.)

To skip the data type tests, set the ``RUN_DATATYPES`` environment
variable to ``0``.

Testing Timeseries (Riak 2.1+)
------------------------------

To test timeseries data, you must run the ``setup_timeseries`` command,
which will create the bucket-types used in testing, or create them
manually yourself. It can be run like so (substituting ``$RIAK`` with
the root of your Riak install)

.. code-block:: console

    ./setup.py setup_timeseries --riak-admin=$RIAK/bin/riak-admin

You may alternately add these lines to `setup.cfg`

.. code-block:: ini

    [setup_timeseries]
    riak-admin=/Users/sean/dev/riak/rel/riak/bin/riak-admin

To enable the timeseries tests, set the ``RUN_TIMESERIES`` environment
variable to ``1``.

Testing Secondary Indexes
-------------------------

To test
`Secondary Indexes <http://docs.basho.com/riak/2.0.0/dev/using/2i/>`_,
the ``RUN_INDEXES`` environment variable must be set to 1 (or 0 to skip them.)

Testing Security (Riak 2+)
--------------------------

By default
`Security <http://docs.basho.com/riak/2.0.0beta1/ops/running/authz/>`_ is not
enabled on Riak.  Once ``security = on`` is configured in the ``riak.conf``
file it can be enabled with ``riak-admin``.

If you have set up the test environment outlined in the `Testing`_ section
you can go ahead and use this command to enable security

.. code-block:: console 

    python setup.py enable_security --riak-admin=$RIAK_DIR/bin/riak-admin

Once you are done testing security you can also

.. code-block:: console

    python setup.py disable_security --riak-admin=$RIAK_DIR/bin/riak-admin

To run the tests, then simply

.. code-block:: console

    RUN_SECURITY=1 RIAK_TEST_HTTP_PORT=18098 python setup.py test

Contributors
--------------------------
   - Rusty Klophaus <rusty@basho.com>
   - Justin Sheehy <justin@basho.com>
   - Jay Baird <jay@mochimedia.com>
   - Andy Gross <andy@basho.com>
   - Jon Meredith <jmeredith@basho.com>
