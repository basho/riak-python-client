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

When you need to work with larger sets of data, one of the tools at your
disposal is MapReduce_. This technique iterates over all of the data, returning
data from the map phase & combining all the different maps in the reduce
phase(s).

.. _MapReduce: http://wiki.basho.com/display/RIAK/MapReduce

To perform a map operation, such as returning all active users, you can do
something like::

  import riak
  
  client = riak.RiakClient()
  # First, you need to ``add`` the bucket you want to MapReduce on.
  query = client.add('user')
  # Then, you supply a Javascript map function as the code to be executed.
  query.map("function(v) { var data = JSON.parse(v.values[0].data); if(data.is_active == true) { return [[v.key, data]]; } return []; }")
  
  for result in query.run():
      # Print the key (``v.key``) and the value for that key (``data``).
      print "%s - %s" % (result[0], result[1])
  
  # Results in something like:
  #
  # mr_smith - {'first_name': 'Mister', 'last_name': 'Smith', 'is_active': True}
  # johndoe - {'first_name': 'John', 'last_name': 'Doe', 'is_active': True}
  # annabody - {'first_name': 'Anna', 'last_name': 'Body', 'is_active': True}

You can also do this manually::

  import riak
  
  client = riak.RiakClient()
  query = riak.RiakMapReduce(client).add('user')
  query.map("function(v) { var data = JSON.parse(v.values[0].data); if(data.is_active == true) { return [[v.key, data]]; } return []; }")
  
  for result in query.run():
      print "%s - %s" % (result[0], result[1])

Adding a reduce phase, say to sort by username (key), looks almost identical::

  import riak
  
  client = riak.RiakClient()
  query = client.add('user')
  query.map("function(v) { var data = JSON.parse(v.values[0].data); if(data.is_active == true) { return [[v.key, data]]; } return []; }")
  query.reduce("function(values) { return values.sort(); }")
  
  for result in query.run():
      # Print the key (``v.key``) and the value for that key (``data``).
      print "%s - %s" % (result[0], result[1])
  
  # Results in something like:
  #
  # annabody - {'first_name': 'Anna', 'last_name': 'Body', 'is_active': True}
  # johndoe - {'first_name': 'John', 'last_name': 'Doe', 'is_active': True}
  # mr_smith - {'first_name': 'Mister', 'last_name': 'Smith', 'is_active': True}


Working With Related Data Via Links
===================================

Links_ are powerful concept in Riak that allow, within the key/value pair's
metadata, relations between objects.

.. _Links: http://wiki.basho.com/display/RIAK/Links

Adding them to your data is relatively trivial. For instance, we'll link a
user's statuses to their user data::

  import riak
  import uuid
  
  client = riak.RiakClient()
  user_bucket = client.bucket('user')
  status_bucket = client.bucket('status')
  
  johndoe = user_bucket.get('johndoe')
  
  new_status = status_bucket.new(uuid.uuid1(), data={
      'message': 'First post!',
      'created': time.time(),
      'is_public': True,
  })
  # Add one direction (from status to user)...
  new_status.add_link(johndoe)
  new_status.store()
  
  # ... Then add the other direction.
  johndoe.add_link(new_status)
  johndoe.store()

Fetching the data is equally simple::

  import riak
  
  client = riak.RiakClient()
  user_bucket = client.bucket('user')
  
  johndoe = user_bucket.get('johndoe')
  
  for status_link in johndoe.get_links():
      # Since what we get back are lightweight ``RiakLink`` objects, we need to
      # get the associated ``RiakObject`` to access its data.
      status = status_link.get()
      print status.get_data()['message']

As usual, it's also possible to do this manually::

  import riak
  import uuid
  
  client = riak.RiakClient()
  user_bucket = client.bucket('user')
  status_bucket = client.bucket('status')
  
  johndoe = user_bucket.get('johndoe')
  
  new_status_key = uuid.uuid1()
  new_status = status_bucket.new(new_status_key, data={
      'message': 'First post!',
      'created': time.time(),
      'is_public': True,
  })
  
  # Add one direction (from status to user)...
  user_link = riak.RiakLink(user_bucket, 'johndoe')
  new_status.add_link(user_link)
  new_status.store()
  
  # ... Then add the other direction.
  status_link = riak.RiakLink(status_bucket, new_status_key)
  johndoe.add_link(status_link)
  johndoe.store()
  
  # Querying looks the same...


Using Search
============

`Riak Search`_ is a new feature available as of Riak 0.13. It allows you to create
queries that filter on data in the values without writing a MapReduce. It takes
inspiration from Lucene_, a popular Java-based search library, and incorporates
a Solr-like interface into Riak. The setup of this is outside the realm of this
tutorial, but usage of this feature looks like::

  import riak
  
  client = riak.RiakClient()
  
  # First parameter is the bucket we want to search within, the second
  # is the query we want to perform.
  search_query = client.search('user', 'first_name:[Anna TO John]')
  
  for result in search_query.run():
      # You get ``RiakLink`` objects back.
      user = result.get()
      user_data = user.get_data()
      print "%s %s" % (user_data['first_name'], user_data['last_name'])
  
  # Results in something like:
  #
  # John Doe
  # Anna Body

.. _`Riak Search`: http://wiki.basho.com/display/RIAK/Riak+Search
.. _Lucene: http://lucene.apache.org/
