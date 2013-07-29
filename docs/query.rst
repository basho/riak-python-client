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
<riak.mapreduce.MapReduce.index>`.

^^^^^^^^^^^^^^^^^^
Riak 1.4+ Features
^^^^^^^^^^^^^^^^^^

.. note:: The features below will raise ``NotImplementedError`` if
   requested against a server that does not support them.

Sometimes the number of results from such a query is too great to
process in one payload, so you can also :meth:`stream the results
<riak.bucket.RiakBucket.stream_index>`::

    for keys in bucket.stream_index("bmonth_int", 1):
        # keys is a list of matching keys
        print keys

Both the regular :meth:`~riak.bucket.RiakBucket.get_index` method and
the :meth:`~riak.bucket.RiakBucket.stream_index` method allow you to
return the index entry along with the matching key as tuples using the
``return_terms`` option::

    bucket.get_index("byear_int", 1970, 1990, return_terms=True)
    # => [(1979, 'seancribbs')]

You can also limit the number of results using the ``max_results``
option, which enables pagination::

   results = bucket.get_index("fname_bin", "S", "T", max_results=20)

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

---------------
Fulltext Search
---------------

If Riak Search is enabled, you can query an index via the bucket's
:meth:`~riak.bucket.RiakBucket.search` method::

    bucket.enable_search()
    bucket.new("one", data={'value':'one'},
               content_type="application/json").store()

    bucket.search('value=one')

To manually add and remove documents from an index (without an
associated key), use the :class:`~riak.client.RiakClient`
:meth:`~riak.client.RiakClient.fulltext_add` and
:meth:`~riak.client.RiakClient.fulltext_delete` methods directly.

---------
MapReduce
---------

.. currentmodule:: riak.mapreduce

:class:`MapReduce` allows you to construct query-processing jobs that
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

^^^^^^^^^
Execution
^^^^^^^^^

Query results can either be executed in one round-trip, or streamed
back to the client. The format of results will depend on the structure
of the ``map`` and ``reduce`` phases the query contains.

.. automethod:: RiakMapReduce.run
.. automethod:: RiakMapReduce.stream
