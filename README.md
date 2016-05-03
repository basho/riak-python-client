Python Client for Riak
======================

Documentation
=============

[Documentation for the Riak Python Client Library](http://basho.github.io/riak-python-client/index.html) is available [here](http://basho.github.io/riak-python-client/index.html).

Documentation for Riak is available [here](http://docs.basho.com/riak/latest).

Repository Cloning
==================

*NOTE*: please clone this repository using the `--recursive` argument to `git clone` or follow the clone with `git submodule update --init`. This repository uses two submodules.

Install
=======

The recommended versions of Python for use with this client are Python `2.7.x`, `3.3.x`, `3.4.x` and `3.5.x`. The latest version from each series should be preferred.

Riak TS (Timeseries)
===================

You must use version `2.7.11`, `3.4.4` or `3.5.1` (or greater within a version series). Otherwise you will be affected by [this Python bug](https://bugs.python.org/issue23517).

From Source
-----------

```sh
python setup.py install
```

There are additional dependencies on Python packages `setuptools` and `protobuf`.

From PyPI
---------

Official packages are signed and published to [PyPI](https://pypi.python.org/pypi/riak).

To install from [PyPI](https://pypi.python.org/pypi/riak) directly you can use `pip`. 

```sh
pip install riak
```

Testing
=======

To setup the default test configuration build a test Riak node (from a `riak` directory)

```sh
make rel
```

See [Basic Cluster Setup](http://docs.basho.com/riak/latest/ops/building/basic-cluster-setup/) for more details.

For all of the simple default values, set the `RIAK_DIR` environment variable to the root of your Riak installation. Then from the `riak-python-client` directory 

```sh
make -C buildbot preconfigure
```

Start your Riak node with `riak start` from the the Riak directory, then 

```sh
make -C buildbot configure
make -C buildbot test
```

That will run the test suite twice: once with security enabled and once without.

Connections to Riak in Tests
----------------------------

If your Riak server isn't running on localhost or you have built a Riak devrel from source, use the environment variables `RIAK_TEST_HOST`, `RIAK_TEST_HTTP_PORT` and `RIAK_TEST_PB_PORT` to specify where to find the Riak server.  `RIAK_TEST_PROTOCOL` to specify which protocol to test. Can be either `pbc` or `http`.

Some of the connection tests need port numbers that are NOT in use. If ports 1023 and 1022 are in use on your test system, set the environment variables `DUMMY_HTTP_PORT` and `DUMMY_PB_PORT` to unused port numbers.

Testing Search
--------------

If you don't have [Riak Search](http://docs.basho.com/riak/latest/dev/using/search/) enabled, you can set the `RUN_SEARCH` environment variable to 0 skip those tests.

If you don't have [Search 2.0](https://github.com/basho/yokozuna) enabled, you can set the `RUN_YZ` environment variable to 0 to skip those tests.

Testing Bucket Types (Riak 2+)
------------------------------

To test bucket-types, you must run the `create_bucket_types` setup command, which will create the bucket-types used in testing, or create them manually yourself. It can be run like so (substituting `$RIAK` with the root of your Riak install)

```sh
./setup.py create_bucket_types --riak-admin=$RIAK/bin/riak-admin
```

You may alternately add these lines to `setup.cfg`

```ini
[create_bucket_types]
riak-admin=/Users/sean/dev/riak/rel/riak/bin/riak-admin
```

To skip the bucket-type tests, set the `RUN_BTYPES` environment variable to `0`.

Testing Data Types (Riak 2+)
----------------------------

To test data types, you must set up bucket types (see above.)

To skip the data type tests, set the `RUN_DATATYPES` environment variable to `0`.

Testing Timeseries (Riak 2.1+)
------------------------------

To test timeseries data, you must run the `setup_timeseries` command, which will create the bucket-types used in testing, or create them manually yourself. It can be run like so (substituting `$RIAK` with the root of your Riak install)

```sh
./setup.py setup_timeseries --riak-admin=$RIAK/bin/riak-admin
```

You may alternately add these lines to `setup.cfg`

```sh
[setup_timeseries]
riak-admin=/Users/sean/dev/riak/rel/riak/bin/riak-admin
```

To enable the timeseries tests, set the `RUN_TIMESERIES` environment variable to `1`.

Testing Secondary Indexes
-------------------------

To test [Secondary Indexes](http://docs.basho.com/riak/latest/dev/using/2i/), the `RUN_INDEXES` environment variable must be set to 1 (or 0 to skip them.)

Testing Security (Riak 2+)
--------------------------

Ensure that the hostname `riak-test` resolves to your Riak host (most likely `localhost`). This is so the SSL host verification can succeed.

By default [Security](http://docs.basho.com/riak/latest/ops/running/authz/) is not enabled on Riak. Once `security = on` is configured in the `riak.conf` file it can be enabled with `riak-admin`.

To run the tests

```sh
RUN_SECURITY=1 RIAK_TEST_HTTP_PORT=18098 python setup.py test
```

Contributors
--------------------------

* Andrew Thompson
* Andy Gross
* Armon Dadgar
* Brett Hazen
* Brett Hoerner
* Brian Roach
* Bryan Fink
* Daniel Lindsley
* Daniel Néri
* Daniel Reverri
* [Dan Root](https://github.com/daroot)
* [David Basden](https://github.com/dbasden)
* David Koblas
* Dmitry Rozhkov
* Eric Florenzano
* Eric Moritz
* Filip de Waard
* Gilles Devaux
* Greg Nelson
* Gregory Burd
* Greg Stein
* Ian Plosker
* Jayson Baird
* Jeffrey Massung
* Jon Meredith
* Josip Lisec
* Justin Sheehy
* Kevin Smith
* [Luke Bakken](https://github.com/lukebakken)
* Mark Erdmann
* Mark Phillips
* Mathias Meyer
* Matt Heitzenroder
* [Matt Lohier](https://github.com/aquam8)
* Mikhail Sobolev
* Reid Draper
* Russell Brown
* Rusty Klophaus
* Rusty Klophaus
* Scott Lystig Fritchie
* Sean Cribbs
* Shuhao Wu
* Silas Sewell
* Socrates Lee
* Soren Hansen
* Sreejith Kesavan
* Timothée Peignier
* [Vitaly Shestovskiy](https://github.com/lamp0chka)
* William Kral
