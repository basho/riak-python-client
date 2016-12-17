=============
Query Methods
=============

Although most operations you will do involve directly interacting with
known buckets and keys, there are additional ways to get information
out of Riak.

-----------------
Secondary Indexes
-----------------

:ref:`Objects <object_accessors>` can be :meth:`tagged
<riak.riak_object.RiakObject.add_index>` with :attr:`secondary index
entries <riak.riak_object.RiakObject.indexes>`. Those entries can then
be queried over :meth:`the bucket <riak.bucket.RiakBucket.get_index>`
for equality or across ranges.::

    bucket = client.bucket("index_test")

    # Tag an object with indexes and save
    sean = bucket.new("seancribbs")
    sean.add_index("fname_bin", "Sean")
    sean.add_index("byear_int", 1979)
    sean.store()

    # Performs an equality query
    seans = bucket.get_index("fname_bin", "Sean")

    # Performs a range query
    eighties = bucket.get_index("byear_int", 1980, 1989)

Secondary indexes are also available via :meth:`MapReduce
<riak.mapreduce.RiakMapReduce.index>`.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Streaming and Paginating Indexes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Sometimes the number of results from such a query is too great to
process in one payload, so you can also :meth:`stream the results
<riak.bucket.RiakBucket.stream_index>`::

    for keys in bucket.stream_index("bmonth_int", 1):
        # keys is a list of matching keys
        print(keys)

Both the regular :meth:`~riak.bucket.RiakBucket.get_index` method and
the :meth:`~riak.bucket.RiakBucket.stream_index` method allow you to
return the index entry along with the matching key as tuples using the
``return_terms`` option::

    bucket.get_index("byear_int", 1970, 1990, return_terms=True)
    # => [(1979, 'seancribbs')]

You can also limit the number of results using the ``max_results``
option, which enables pagination::

   results = bucket.get_index("fname_bin", "S", "T", max_results=20)

Optionally you can use :meth:`~riak.bucket.RiakBucket.paginate_index`
or :meth:`~riak.bucket.RiakBucket.paginate_stream_index` to create a
generator of paged results::

   for page in bucket.paginate_stream_index("maestro_bin", "Cribbs"):
       for key in page:
           do_something(key)
       page.close()

All of these features are implemented using the
:class:`~riak.client.index_page.IndexPage` class, which emulates a
list but also supports streaming and capturing the
:attr:`~riak.client.index_page.IndexPage.continuation`, which is a
sort of pointer to the next page of results::

   # Detect whether there are more results
   if results.has_next_page():

       # Fetch the next page of results manually
       more = bucket.get_index("fname_bin", "S", "T", max_results=20,
                               continuation=results.continuation)

       # Fetch the next page of results automatically
       more = results.next_page()

.. currentmodule:: riak.client.index_page

.. autoclass:: IndexPage

   .. autoattribute:: continuation
   .. automethod:: has_next_page
   .. automethod:: next_page
   .. automethod:: __eq__
   .. automethod:: __iter__
   .. automethod:: __getitem__

---------
MapReduce
---------

.. currentmodule:: riak.mapreduce

:class:`RiakMapReduce` allows you to construct query-processing jobs that
are performed mostly in-parallel around the Riak cluster. You can
think of it as a pipeline, where inputs are fed in one end, they pass
through a number of ``map`` and ``reduce`` phases, and then are
returned to the client.

^^^^^^^^^^^^^^^^^^^^^^
Constructing the query
^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: RiakMapReduce

^^^^^^
Inputs
^^^^^^

The first step is to identify the inputs that should be processed.
They can be:

#. An entire :meth:`bucket <RiakMapReduce.add_bucket>`
#. An entire bucket, with the :meth:`keys filtered by criteria <RiakMapReduce.add_key_filters>`
#. A :meth:`list of bucket/key pairs <RiakMapReduce.add>` or bucket/key/data triples
#. A :meth:`fulltext search query <RiakMapReduce.search>`
#. A :meth:`secondary-index query <RiakMapReduce.index>`

Adding inputs always returns the ``RiakMapReduce`` object so that you
can chain the construction of the query job.

.. automethod:: RiakMapReduce.add_bucket
.. automethod:: RiakMapReduce.add_key_filters
.. automethod:: RiakMapReduce.add_key_filter

.. automethod:: RiakMapReduce.add
.. automethod:: RiakMapReduce.add_object
.. automethod:: RiakMapReduce.add_bucket_key_data

.. automethod:: RiakMapReduce.search
.. automethod:: RiakMapReduce.index

.. autoclass:: RiakKeyFilter

^^^^^^
Phases
^^^^^^

The second step is to add processing phases to the query. ``map``
phases load and process individual keys, returning one or more
results, while ``reduce`` phases operate over collections of results
from previous phases. ``link`` phases are a special type of ``map``
phase that extract matching :attr:`~riak.riak_object.RiakObject.links`
from the object, usually so they can be used in a subsequent ``map``
phase.

Any number of phases can return results directly to the client by
passing ``keep=True``.

.. automethod:: RiakMapReduce.map
.. automethod:: RiakMapReduce.reduce
.. automethod:: RiakMapReduce.link

.. autoclass:: RiakMapReducePhase

.. autoclass:: RiakLinkPhase

"""""""""""""""
Phase shortcuts
"""""""""""""""

A number of commonly-used phases are also available as shortcut
methods:

.. automethod:: RiakMapReduce.map_values
.. automethod:: RiakMapReduce.map_values_json
.. automethod:: RiakMapReduce.reduce_sum
.. automethod:: RiakMapReduce.reduce_min
.. automethod:: RiakMapReduce.reduce_max
.. automethod:: RiakMapReduce.reduce_sort
.. automethod:: RiakMapReduce.reduce_numeric_sort
.. automethod:: RiakMapReduce.reduce_limit
.. automethod:: RiakMapReduce.reduce_slice
.. automethod:: RiakMapReduce.filter_not_found

^^^^^^^^^
Execution
^^^^^^^^^

Query results can either be executed in one round-trip, or streamed
back to the client. The format of results will depend on the structure
of the ``map`` and ``reduce`` phases the query contains.

.. automethod:: RiakMapReduce.run
.. automethod:: RiakMapReduce.stream

^^^^^^^^^^^^^^^^^^^^^
Shortcut constructors
^^^^^^^^^^^^^^^^^^^^^

:class:`~riak.riak_object.RiakObject` contains some shortcut methods
that make it more convenient to begin constructing
:class:`RiakMapReduce` queries.

.. currentmodule:: riak.riak_object

.. automethod:: RiakObject.add
.. automethod:: RiakObject.link
.. automethod:: RiakObject.map
.. automethod:: RiakObject.reduce

.. _yz-label:

--------------------------
Riak Search 2.0 (Yokozuna)
--------------------------

With Riak 2.0 came the introduction of **Riak Search 2.0**, a.k.a `Yokozuna`
(the top rank in sumo).  Riak Search 2.0 is an integration of Solr (for
indexing and querying) and Riak (for storage and distribution).
It allows for distributed, scalable,
fault-tolerant, transparent indexing and querying of Riak values.
After connecting a bucket (or bucket type) to a
`Apache Solr <http://lucene.apache.org/solr>`_ index, you simply write
values (such as JSON, XML, plain text, Data Types, etc.) into Riak as
normal, and then query those indexed values using the Solr API.
Unlike traditional Riak data, however, Solr needs to know the format
of the stored data so it can index it.  Solr is a document-based
search engine so it treats each value stored in Riak as a document.

^^^^^^^^^^^^^^^^^
Creating a schema
^^^^^^^^^^^^^^^^^

The first thing which needs to be done is to define a Solr schema for
your data.  Riak Search comes bundled with a default schema named
``_yz_default``. It defaults to many dynamic field types, where the
suffix defines its type. This is an easy path to start development,
but we recommend in production that you define your own schema.

You can find information about defining your own schema at
`Search Schema
<http://docs.basho.com/riak/2.0.0/dev/advanced/search-schema>`_,
with a short section dedicated to the `default schema
<http://docs.basho.com/riak/2.0.0/dev/advanced/search-schema/#The-Default-Schema>`_.

Here is a brief example of creating a custom schema with
:meth:`~riak.client.RiakClient.create_search_schema`::

    content = """<?xml version="1.0" encoding="UTF-8" ?>
    <schema name="test" version="1.5">
    <fields>
       <field name="_yz_id" type="_yz_str" indexed="true" stored="true"
        multiValued="false" required="true" />
       <field name="_yz_ed" type="_yz_str" indexed="true" stored="true"
        multiValued="false" />
       <field name="_yz_pn" type="_yz_str" indexed="true" stored="true"
        multiValued="false" />
       <field name="_yz_fpn" type="_yz_str" indexed="true" stored="true"
        multiValued="false" />
       <field name="_yz_vtag" type="_yz_str" indexed="true" stored="true"
        multiValued="false" />
       <field name="_yz_rk" type="_yz_str" indexed="true" stored="true"
        multiValued="false" />
       <field name="_yz_rb" type="_yz_str" indexed="true" stored="true"
        multiValued="false" />
       <field name="_yz_rt" type="_yz_str" indexed="true" stored="true"
        multiValued="false" />
       <field name="_yz_err" type="_yz_str" indexed="true"
        multiValued="false" />
    </fields>
    <uniqueKey>_yz_id</uniqueKey>
    <types>
        <fieldType name="_yz_str" class="solr.StrField"
         sortMissingLast="true" />
    </types>
    </schema>"""
    schema_name = 'jalapeno'
    client.create_search_schema(schema_name, content)

If you would like to retrieve the current XML Solr schema,
:meth:`~riak.client.RiakClient.get_search_schema` is available::

    schema = client.get_search_schema('jalapeno')

^^^^^^^^^^^^
Solr indexes
^^^^^^^^^^^^

Once a schema has been created, then a Solr index must also be created.
This index represents a collection of similar data that you use to perform
queries. When creating an index with
:meth:`~riak.client.RiakClient.create_search_index`, you can optionally
specify a schema. If you do not, the default schema will be used::

    client.create_search_index('nacho')

Likewise you can specify a schema, e.g. the index ``"nacho"`` is
associated with the schema ``"jalapeno"``::

    client.create_search_index('nacho', 'jalapeno')

Just as easily you can delete an index with
:meth:`~riak.client.RiakClient.delete_search_index`::

    client.delete_search_index('jalapeno')

A single index can be retrieved with
:meth:`~riak.client.RiakClient.get_search_index` or all of them
with :meth:`~riak.client.RiakClient.list_search_indexes`::

    index = client.get_search_index('jalapeno')
    name = index['name']
    schema = index['schema']
    indexes = client.list_search_indexes()
    first_nval = indexes[0]['n_val']

.. note:: Note that index names may only be ASCII values from 32-127
          (spaces, standard punctuation, digits and word characters).
          This may change in the future to allow full unicode support.

More discussion about Riak Search 2.0 Indexes can be found at `Indexes
<http://docs.basho.com/riak/2.0.0/dev/advanced/search/#Indexes>`_.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Linking a bucket type to an index
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The last step to setting up Riak Search 2.0 is to link a Bucket Type
to a Solr index.  This lets Riak know when to index values.  This can be
done via the command line::

   riak-admin bucket-type create spicy '{"props":{"search_index":"jalapeno"}}'
   riak-admin bucket-type activate spicy

Or simply create an empty Bucket Type::

   riak-admin bucket-type create spicy '{"props":{}}'
   riak-admin bucket-type activate spicy

Then change the bucket properties on the associated bucket or Bucket Type::

   b = client.bucket('peppers')
   b.set_property('search_index', 'jalapeno')
   btype = client.bucket_type('spicy')
   btype.set_property('search_index', 'jalapeno')

^^^^^^^^^^^^^^^^^
Querying an index
^^^^^^^^^^^^^^^^^

Once the schema, index and bucket properties have all been properly configured,
adding data is as simple as writing to Riak.  Solr is automatically updated.

To query, on the other hand, is as easy as writing Solr queries.  This allows
for the full use of existing Solr tools as well as its rich semantics.

Here is a brief example of loading and querying data:::

    bucket = self.client.bucket('peppers')
    bucket.new("bell", {"name_s": "bell", "scoville_low_i": 0,
                        "scoville_high_i": 0}).store()
    bucket.new("anaheim", {"name_s": "anaheim", "scoville_low_i": 1000,
                           "scoville_high_i": 2500}).store()
    bucket.new("chipotle", {"name_s": "chipotle", "scoville_low_i": 3500,
                            "scoville_high_i": 10000}).store()
    bucket.new("serrano", {"name_s": "serrano", "scoville_low_i": 10000,
                           "scoville_high_i": 23000}).store()
    bucket.new("habanero", {"name_s": "habanero", "scoville_low_i": 100000,
                            "scoville_high_i": 350000}).store()
    results = bucket.search("name_s:/c.*/", index='jalapeno')
    # Yields single document 'chipotle'
    print(results['docs'][0]['name_s'])
    results = bucket.search("scoville_high_i:[20000 TO 500000]")
    # Yields two documents
    for result in results['docs']:
        print(result['name_s'])
    results = bucket.search('name_s:*', index='jalapeno', 
                            sort="scoville_low_i desc")
    # Yields all documents, sorted in descending order. We take the top one
    print("The hottest pepper is {0}".format(results['docs'][0]['name_s']))

The results returned by :meth:`~riak.bucket.RiakBucket.search` is a dictionary
with lots of search metadata like the number of results, the maxium
`Lucene Score
<https://lucene.apache.org/core/4_9_0/core/org/apache/lucene/search/package-summary.html#scoring>`_
as well as the matching documents.

When querying on :ref:`datatypes` the datatype is the name of the field
used in Solr since they do not fit into the default schema, e.g.:

.. code::

   riak-admin bucket-type create visitors '{"props":{"datatype": "counter}}'
   riak-admin bucket-type activate visitors

.. code:: python

   client.create_search_index('website')
   bucket = client.bucket_type('visitors').bucket('hits')
   bucket.set_property('search_index', 'website')

   site = bucket.new('bbc.co.uk')
   site.increment(80)
   site.store()
   site = bucket.new('cnn.com')
   site.increment(150)
   site.store()
   site = bucket.new('abc.net.au')
   site.increment(24)
   site.store()

   results = bucket.search("counter:[10 TO *]", index='website',
                           sort="counter desc", rows=5)

   # Assume you have a bucket-type named "profiles" that has datatype
   # "map". Let's create and search an index containing maps.
   client.create_search_index('user-profiles')
   bucket = client.bucket_type('profiles').bucket('USA')
   bucket.set_property('search_index', 'user-profiles')

   brett = bucket.new()
   brett.registers['fname'].assign("Brett")
   brett.registers['lname'].assign("Hazen")
   brett.sets['emails'].add('spam@basho.com')
   brett.counters['visits'].increment()
   brett.maps['pages'].counters['homepage'].increment()
   brett.update()

   # Note that the field name in the index/schema is the field name in
   # the map joined with its type by an underscore. Deeply embedded
   # fields are joined with their parent field names by an underscore.
   results = bucket.search('lname_register:Hazen AND pages_map_homepage_counter:[1 TO *]',
                           index='user-profiles')
   

Details on querying Riak Search 2.0 can be found at `Querying
<http://docs.basho.com/riak/2.0.0/dev/using/search/#Querying>`_.
                            
