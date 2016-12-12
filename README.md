# Python Client for Riak

## Build Status

[![Build Status](https://travis-ci.org/basho/riak-python-client.svg?branch=master)](https://travis-ci.org/basho/riak-python-client)

## Documentation

[Documentation for the Riak Python Client Library](http://basho.github.io/riak-python-client/index.html) is available [here](http://basho.github.io/riak-python-client/index.html).

Documentation for Riak is available [here](http://docs.basho.com/riak/latest).

## Repository Cloning

*NOTE*: please clone this repository using the `--recursive` argument to `git clone` or follow the clone with `git submodule update --init`. This repository uses two submodules.

# Installation

The recommended versions of Python for use with this client are Python `2.7.8` (or greater, `2.7.11` as of `2016-06-21`), `3.3.x`, `3.4.x` and `3.5.x`. The latest version from each series should be preferred. Older versions of the Python `2.7.X` and `3.X` series should be used with caution as they are not covered by integration tests.

## Riak TS (Timeseries)

You must use version `2.7.11`, `3.4.4` or `3.5.1` (or greater within a version series). Otherwise you will be affected by [this Python bug](https://bugs.python.org/issue23517).

## From Source

```sh
python setup.py install
```

There are additional dependencies on Python packages `setuptools` and `protobuf`.

## From PyPI

Official packages are signed and published to [PyPI](https://pypi.python.org/pypi/riak).

To install from [PyPI](https://pypi.python.org/pypi/riak) directly you can use `pip`. 

```sh
pip install riak
```

# Testing

## Unit Tests

Unit tests will be executed via `tox` if it is in your `PATH`, otherwise by the `python2` and (if available), `python3` executables:

```sh
make unit-test
```

## Integration Tests

You have two options to run Riak locally - either build from source, or use a pre-installed Riak package.

### Source

To setup the default test configuration, build a Riak node from a clone of `github.com/basho/riak`:

```sh
# check out latest release tag
git checkout riak-2.1.4
make locked-deps
make rel
```

[Source build documentation](http://docs.basho.com/riak/kv/latest/setup/installing/source/).

When building from source, the protocol buffers port will be `8087` and HTTP will be `8098`.

### Package

Install using your platform's package manager ([docs](http://docs.basho.com/riak/kv/latest/setup/installing/))

When installing from a package, the protocol buffers port will be `8087` and HTTP will be `8098`.

### Running Integration Tests

* Ensure you've initialized this repo's submodules:

```sh
git submodule update --init
```

* Run the following:

```sh
./tools/setup-riak
make integration-test
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
* [David Delassus](https://github.com/linkdd)
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
* [`tobixx`](https://github.com/tobixx)
* [Tin Tvrtković](https://github.com/Tinche)
* [Vitaly Shestovskiy](https://github.com/lamp0chka)
* William Kral
* [Yasser Souri](https://github.com/yassersouri)
