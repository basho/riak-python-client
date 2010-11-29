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


Storing Keys/Values
===================

Once you've got a working client/bucket, the next task at hand is storing data.
Riak provides several ways to store your data, but the most common are a
JSON-encoded structure or a binary blob.

To store JSON-encoded data, you'd do something like the following::

  import riak
  
  client = riak.RiakClient()
  user_bucket = client.bucket('user')
  
  # We're creating the user data & keying off their username.
  new_user = user_bucket.new('johndoe', data={
      'first_name': 'John',
      'last_name': 'Doe',
      'gender': 'm',
      'website': 'http://example.com/',
      'is_active': True,
  })
  # Note that the user hasn't been stored in Riak yet.
  new_user.store()

Note that any data Python's ``json`` (or ``simplejson``) encoder can handle is
fair game.

As mentioned, Riak can also handle binary data, such as images, audio files,
etc. Storing binary data looks almost identical::

  import riak
  
  client = riak.RiakClient()
  user_photo_bucket = client.bucket('user_photo')
  
  # For example purposes, we'll read a file off the filesystem, but you can get
  # the data from anywhere.
  the_photo_data = open('/tmp/johndoe_headshot.jpg', 'rb').read()
  
  # We're storing the photo in a different bucket but keyed off the same
  # username.
  new_user = user_bucket.new_binary('johndoe', data=the_photo_data, content_type='image/jpeg')
  new_user.store()

You can also manually store data by using ``RiakObject``::

  import riak
  import time
  import uuid
  
  client = riak.RiakClient()
  status_bucket = client.bucket('status')
  
  # We use ``uuid.uuid1()`` here to create a unique identifier for the status.
  new_status = RiakObject(client, status_bucket, uuid.uuid1())
  
  # Add in the data you want to store.
  new_status.set_data({
      'message': 'First post!',
      'created': time.time(),
      'is_public': True,
  })
  
  # Set the content type.
  new_status.set_content_type('application/json')
  
  # We want to do JSON-encoding on the value.
  new_status._encode_data = True
  
  # Again, make sure you save it.
  new_status.store()


Getting Single Values Out
=========================

Storing data is all well and good, but you'll need to get that data out at a
later date.

Riak provides several ways to get data out, though fetching single key/value
pairs is the easiest. Just like storing the data, you can pull the data out
in either the JSON-decoded form or a binary blob. Getting the JSON-decoded
data out looks like::

  import riak
  
  client = riak.RiakClient()
  user_bucket = client.bucket('user')
  
  johndoe = user_bucket.get('johndoe')
  
  # You've now got a ``RiakObject``. To get at the values in a dictionary
  # form, call:
  johndoe_dict = johndoe.get_data()

Getting binary data out looks like::

  import riak
  
  client = riak.RiakClient()
  user_photo_bucket = client.bucket('user_photo')
  
  johndoe = user_photo_bucket.get_binary('johndoe')
  
  # You've now got a ``RiakObject``. To get at the binary data, call:
  johndoe_headshot = johndoe.get_data()

Manually fetching data is also possible::

  import riak
  
  client = riak.RiakClient()
  status_bucket = client.bucket('status')
  
  # We're using the UUID generated from the above section.
  first_post_status = RiakObject(client, status_bucket, '39fbee54-fb82-11df-a2cf-d49a20c04e6a')
  first_post_status._encode_data = True
  r = status_bucket.get_r(r)
  
  # Calling ``reload`` will cause the ``RiakObject`` instance to load fresh
  # data/metadata from Riak.
  first_post_status.reload(r)
  
  # Finally, pull out the data.
  message = first_post_status.get_data()['message']


Fetching Data Via Map/Reduce
============================

TBD.


Working With Related Data Via Links
===================================

TBD.


Using Search
============

TBD.
