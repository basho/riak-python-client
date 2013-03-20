========================
Python Client for Riak
========================

.. image:: https://secure.travis-ci.org/basho/riak-python-client.png?branch=master
   :target: http://travis-ci.org/basho/riak-python-client

Documentation
==============

`Documentation for the Riak Python Client Library <http://basho.github.com/riak-python-client/index.html>`_ is available here.
The documentation source is found in `docs/ subdirectory
<https://github.com/basho/riak-python-client/tree/master/docs>`_ and can be
built with `Sphinx <http://sphinx.pocoo.org/>`_.

Documentation for Riak is available at http://wiki.basho.com/Riak.html

Install
=======

The recommended version of Python for use with this client is Python 2.7.

You must have `Protocol Buffers`_ installed before you can install the Riak Client. From the Riak Python Client root directory, execute::

    python setup.py install

There is an additional dependency on the Python package `setuptools`.  Please install `setuptools` first, e.g. ``port install py27-setuptools`` for OS X and MacPorts.

Unit Test
===========
To run the unit tests against a Riak server (with default TCP port configuration) on localhost, execute::

    python setup.py test

If you don't have `Riak Search <http://wiki.basho.com/Riak-Search.html>`_ enabled you can set the ``SKIP_SEARCH`` environment variable to skip that tests.

If your Riak server isn't running on localhost, use the environment variables ``RIAK_TEST_HOST`` and  ``RIAK_TEST_HTTP_PORT`` and  ``RIAK_TEST_PB_PORT=8087`` to specify where to find the Riak server.

========
Tutorial
========

This tutorial assumes basic working knowledge of how Riak works & what it can
do. If you need a more comprehensive overview how to use Riak, please check out
the `Riak Fast Track`_.

.. _`Riak Fast Track`: http://wiki.basho.com/The-Riak-Fast-Track.html


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

The constructor also configuration options such as ``host``, ``http_port``,
``pb_port`` & ``prefix``. Please refer to the :doc:`client` documentation
for full details.

To use the Protocol Buffers interface::

    import riak

    client = riak.RiakClient(port=8087, transport_class=riak.RiakPbcTransport)

.. warning:

  Riak's default port is 8098. However, when using the Protocol Buffers, the
  Riak listens on port 8087. If you forget this, you will *NOT* get an
  immediate error, but will instead receive an error when fetching or storing
  data to the effect of ``RiakError: 'Socket returned short read 135 -
  expected 8192'``.

The ``transport_class`` argument indicates to the client which transport protocol to use.
We didn't need to specify it in the HTTP example because ``RiakHttpConnection`` is the
default class. Available options are: ``RiakHttpConnection`` and ``RiakPbcTransport``.


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
  new_user = user_photo_bucket.new_binary('johndoe', data=the_photo_data, content_type='image/jpeg')
  new_user.store()

You can also manually store data by using ``RiakObject``::

  import riak
  import time
  import uuid

  client = riak.RiakClient()
  status_bucket = client.bucket('status')

  # We use ``uuid.uuid1().hex`` here to create a unique identifier for the status.
  post_uuid = uuid.uuid1().hex
  new_status = riak.RiakObject(client, status_bucket, post_uuid)

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
  first_post_status = riak.RiakObject(client, status_bucket, post_uuid)
  first_post_status._encode_data = True
  r = status_bucket.get_r()

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

.. _MapReduce: http://wiki.basho.com/MapReduce.html

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

.. _Links: http://wiki.basho.com/Links.html

Adding them to your data is relatively trivial. For instance, we'll link a
user's statuses to their user data::

  import riak
  import uuid

  client = riak.RiakClient()
  user_bucket = client.bucket('user')
  status_bucket = client.bucket('status')

  johndoe = user_bucket.get('johndoe')

  new_status = status_bucket.new(uuid.uuid1().hex, data={
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

You can enable and disable search for specific buckets through convenience
methods that install/remove the precommit hook

  bucket = client.bucket('search')

  if bucket.search_enabled():
    bucket.disable_search()
  else:
    bucket.enable_search()

Search using the Solr Interface
-------------------------------

The search as outlined above goes through Riak's MapReduce facilities to find
and fetch objects. Sometimes you either want to go through the Solr-like
interface Riak Search offers, e.g. to index and search documents without storing
them in Riak KV and relying on the pre-commit hook to index.

Using the Solr interface also allows you to specify sort and limit parameters,
which, using the search based on MapReduce, you'd have to do that with reduce
functions.

You can index documents into search indexes as simple Python dicts, which need
to have an attribute named "id"::

    client = riak.RiakClient()
    client.solr().add("user", {"id": "anna", "first_name": "Anna"})

To search for documents, specify the index and a query string::

    client = riak.RiakClient()
    client.solr().search("user", "first_name:Anna")

Additionally you can specify all the parameters supported by the Solr
interface::

    client.solr().search("user", "Anna", wt="json", df="first_name")

The search interface supports both XML and JSON, parsing both result formats
into dicts.

You can also remove documents from the index again, using either a list of
document ids or queries::

    client.solr().delete("user", docs=["anna"], queries=["first_name:Anna"])

.. _`Riak Search`: http://wiki.basho.com/Riak-Search.html
.. _Lucene: http://lucene.apache.org/
.. _`Riak Search - Querying via the Solr Interface`: http://wiki.basho.com/Riak-Search---Querying.html#Querying-via-the-Solr-Interface

Using Key Filters
==================

`Key filters`_ are a new feature available as of Riak 0.14.  They are
a way to pre-process MapReduce inputs from a full bucket query simply
by examining the key — without loading the object first. This is
especially useful if your keys are composed of domain-specific
information that can be analyzed at query-time.

To illustrate this, let’s contrive an example. Let’s say we’re storing
customer invoices with a key constructed from the customer name and
the date, in a bucket called “invoices”. Here are some sample keys::

    basho-20101215
    google-20110103
    yahoo-20090613

To query all invoices for a given customer::

    import riak
    
    client = riak.RiakClient()
    
    query = client.add("invoices")
    query.add_key_filter("tokenize", "-", 1)
    query.add_key_filter("eq", "google")

    query.map("""function(v) {
        var data = JSON.parse(v.values[0].data);
        return [[v.key, data]];
    }""")
    
   
Alternatively, you can use riak.key_filter to build key filters::

    query.add_key_filters(key_filter.tokenize("-", 1).eq("google"))

Boolean operators can be used with riak.f instances::

    # Query basho's orders for 2010
    filters = key_filter.tokenize("-", 1).eq("basho")\
            & key_filter.tokenize("-", 2).starts_with("2010")

Filters can be combined using the + operator to produce very complex
filters::

    # Query invoices for basho or google
    filters = key_filter.tokenize("-", 1) + (key_filter.eq("basho") | key_filter.eq("google"))

    # This is the same as the following key filters
    [['tokenize', '-', 1], ['or', [['eq', 'google']], [['eq', 'yahoo']]]]


.. _`Key filters`: http://wiki.basho.com/Key-Filters.html

Test Server
===========

The client includes a Riak test server that can be used to start a Riak instance
on demand for testing purposes in your application. It uses in-memory storage
backends for both Riak KV and Riak Search and is therefore reasonably fast for a
testing setup. The in-memory setups also make it easier to wipe all data in the
instance without having to list and delete all keys manually. The original code
comes from Ripple_, as do the file system implementations.

The server needs a local Riak installation, of which it uses only the installed
Erlang libraries and the configuration files to generate and run a temporary
server in a different directory. Make sure you run the most recent stable
version of Riak, and not a development snapshot, where your mileage may vary.

By default, the HTTP port is set to 9000 and the Protocol Buffers interface
listens on port 9001.

To use it, simply point it to your local Riak installation, and the rest is done
automagically::

    from riak.test_server import TestServer

    server = TestServer(bin_dir="/usr/local/riak/0.14.2/bin")
    server.prepare()
    server.start()

The server is started as an external process, with communication going through
the Erlang console. That allows it to easily wipe the in-memory backends used by
Riak and Riak Search. You can use the recycle() method to clean up the server::

    server.recycle()

To change the default configuration, you can specify additional arguments for
the Erlang VM. Let's raise the maximum number of processes to 1000000, just for
fun::

    server = TestServer(vm_args={"+P": "1000000"})

You can also change the default configuration used to generate the app.config
file for the Riak instance. The format of the attributes follows the convention
of the app.config file itself, using a dict with keys for every section in the
configuration file, so "riak_core", "riak_kv", and so on. These in turn are also
dicts, following the same key-value format of the app.config file.

So to change the default HTTP port to 8080, you can do the following::

    server = TestServer(riak_core={"web_port": 8080})

The server should shut down properly when you stop the Python process, but if
you only need it for a subset of your tests, just stop the server::

    server.stop()

If you plan on repeatedly running the test server, either in multiple test
suites or in subsequent test runs, be sure to call cleanup() before starting or
after stopping it.

.. _Ripple: https://github.com/seancribbs/ripple
