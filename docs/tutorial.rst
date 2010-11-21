.. ref-tutorial:

========
Tutorial
========

This tutorial assumes basic working knowledge of how Riak works & what it can
do. If you need a more comprehensive overview how to use Riak, please check out
the `Riak Fast Track`_.

.. _`Riak Fast Track`: http://wiki.basho.com/display/RIAK/The+Riak+Fast+Track


Quick Start
===========

For the impatient, simple usage of the official Python binding for Riak looks
like::

    import riak
    
    # Connect to Riak.
    client = riak.RiakClient()
    
    # Choose the bucket to store data in.
    bucket = client.bucket('test')
    
    
    # Supply a key to store data under.
    # The ``data`` can be any data Python's ``json`` encoder can handle.
    person = bucket.new('riak_developer_1', data={
        'name': 'John Smith',
        'age': 28,
        'company': 'Mr. Startup!',
    })
    # Save the object to Riak.
    person.store()


Connecting To Riak
==================

There are two supported ways to connect to Riak, the HTTP interface & the
`Protocol Buffers`_ interface. Both provide the same API & full access to
Riak.

The HTTP interface is easier to setup & is well suited for development use. It
is the slower of the two interfaces, but if you are only making a handful of
requests, it is more than capable.

The Protocol Buffers (also called ``protobuf``) is more difficult to setup but
is significantly faster (2-3x) and is more suitable for production use. This
interface is better suited to a higher number of requests.

.. _`Protocol Buffers`: http://code.google.com/p/protobuf/

To use the HTTP interface and connecting to a local Riak on the default port,
no arguments are needed::

    import riak
    
    client = riak.RiakClient()

The constructor also configuration options such as ``host``, ``port`` &
``prefix``. Please refer to the :doc:`client` documentation for full details.

To use the Protocol Buffers interface::

    import riak
    
    client = riak.RiakClient(port=8087, transport_class=riak.RiakPbcTransport)

.. warning:

  Riak's default port is 8098. However, when using the Protocol Buffers, the
  Riak listens on port 8087. If you forget this, you will *NOT* get an
  immediate error, but will instead receive an error when fetching or storing
  data to the effect of ``RiakError: 'Socket returned short read 135 -
  expected 8192'``.

The ``transport_class`` argument indicates to the client which backend to use.
We didn't need to specify it in the HTTP example because
``riak.RiakHttpTransport`` is the default class.


Using Buckets
=============

Buckets in Riak's terminology are segmented keyspaces. They are a way to
categorize different types of data and are roughly analogous to tables in an
RDBMS.

Once you have a ``client``, selecting a bucket is simple. Provide a string of
the name of the bucket to use::

    test_bucket = client.bucket('test')

If the bucket does not exist, Riak will create it for you. You can also open
as many buckets as you need::

    user_bucket = client.bucket('user')
    profile_bucket = client.bucket('profile')
    status_bucket = client.bucket('status')

If needed, you can also manually instantiate a bucket like so::

    user_bucket = riak.RiakBucket(client, 'user')

The buckets themselves provide many different methods. The most commonly used
are:

* ``get`` - Fetches a key's value (decoded from JSON).
* ``get_binary`` - Also fetches a key's raw value (plain text or binary).
* ``new`` - Creates a new key/value pair (encoded in JSON).
* ``new_binary`` - Creates a new key/raw value pair.

See the full :doc:`bucket` documentation for the other methods.
