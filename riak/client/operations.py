import six

import riak.client.multi

from riak.client.transport import RiakClientTransport, \
    retryable, retryableHttpOnly
from riak.client.index_page import IndexPage
from riak.datatypes import TYPES
from riak.table import Table
from riak.util import bytes_to_str


class RiakClientOperations(RiakClientTransport):
    """
    Methods for RiakClient that result in requests sent to the Riak
    cluster.

    Note that many of these methods have an implicit 'transport'
    argument that will be prepended automatically as part of the retry
    logic, and does not need to be supplied by the user.
    """

    @retryable
    def get_buckets(self, transport, bucket_type=None, timeout=None):
        """
        get_buckets(bucket_type=None, timeout=None)

        Get the list of buckets as :class:`RiakBucket
        <riak.bucket.RiakBucket>` instances.

        .. warning:: Do not use this in production, as it requires
           traversing through all keys stored in a cluster.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param bucket_type: the optional containing bucket type
        :type bucket_type: :class:`~riak.bucket.BucketType`
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :rtype: list of :class:`RiakBucket <riak.bucket.RiakBucket>`
                instances
        """
        _validate_timeout(timeout)
        if bucket_type:
            bucketfn = self._bucket_type_bucket_builder
        else:
            bucketfn = self._default_type_bucket_builder

        return [bucketfn(bytes_to_str(name), bucket_type) for name in
                transport.get_buckets(bucket_type=bucket_type,
                                      timeout=timeout)]

    def stream_buckets(self, bucket_type=None, timeout=None):
        """
        Streams the list of buckets. This is a generator method that
        should be iterated over.

        .. warning:: Do not use this in production, as it requires
           traversing through all keys stored in a cluster.

        The caller should explicitly close the returned iterator,
        either using :func:`contextlib.closing` or calling ``close()``
        explicitly. Consuming the entire iterator will also close the
        stream. If it does not, the associated connection might not be
        returned to the pool. Example::

            from contextlib import closing

            # Using contextlib.closing
            with closing(client.stream_buckets()) as buckets:
                for bucket_list in buckets:
                    do_something(bucket_list)

            # Explicit close()
            stream = client.stream_buckets()
            for bucket_list in stream:
                 do_something(bucket_list)
            stream.close()

        :param bucket_type: the optional containing bucket type
        :type bucket_type: :class:`~riak.bucket.BucketType`
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :rtype: iterator that yields lists of :class:`RiakBucket
             <riak.bucket.RiakBucket>` instances

        """
        _validate_timeout(timeout)

        if bucket_type:
            bucketfn = self._bucket_type_bucket_builder
        else:
            bucketfn = self._default_type_bucket_builder

        def make_op(transport):
            return transport.stream_buckets(
                    bucket_type=bucket_type, timeout=timeout)

        for bucket_list in self._stream_with_retry(make_op):
            bucket_list = [bucketfn(bytes_to_str(name), bucket_type)
                           for name in bucket_list]
            if len(bucket_list) > 0:
                yield bucket_list

    @retryable
    def ping(self, transport):
        """
        ping()

        Check if the Riak server for this ``RiakClient`` instance is alive.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :rtype: boolean
        """
        return transport.ping()

    is_alive = ping

    @retryable
    def get_index(self, transport, bucket, index, startkey, endkey=None,
                  return_terms=None, max_results=None, continuation=None,
                  timeout=None, term_regex=None):
        """
        get_index(bucket, index, startkey, endkey=None, return_terms=None,\
                  max_results=None, continuation=None, timeout=None,\
                  term_regex=None)

        Queries a secondary index, returning matching keys.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param bucket: the bucket whose index will be queried
        :type bucket: RiakBucket
        :param index: the index to query
        :type index: string
        :param startkey: the sole key to query, or beginning of the query range
        :type startkey: string, integer
        :param endkey: the end of the query range (optional if equality)
        :type endkey: string, integer
        :param return_terms: whether to include the secondary index value
        :type return_terms: boolean
        :param max_results: the maximum number of results to return (page size)
        :type max_results: integer
        :param continuation: the opaque continuation returned from a
            previous paginated request
        :type continuation: string
        :param timeout: a timeout value in milliseconds, or 'infinity'
        :type timeout: int
        :param term_regex: a regular expression used to filter index terms
        :type term_regex: string
        :rtype: :class:`~riak.client.index_page.IndexPage`
        """
        _validate_timeout(timeout, infinity_ok=True)

        page = IndexPage(self, bucket, index, startkey, endkey,
                         return_terms, max_results, term_regex)

        results, continuation = transport.get_index(
            bucket, index, startkey, endkey, return_terms=return_terms,
            max_results=max_results, continuation=continuation,
            timeout=timeout, term_regex=term_regex)

        page.results = results
        page.continuation = continuation
        return page

    def paginate_index(self, bucket, index, startkey, endkey=None,
                       max_results=1000, return_terms=None,
                       continuation=None, timeout=None, term_regex=None):
        """
        Iterates over a paginated index query. This is equivalent to calling
        :meth:`get_index` and then successively calling
        :meth:`~riak.client.index_page.IndexPage.next_page` until all
        results are exhausted.

        Because limiting the result set is necessary to invoke pagination,
        the ``max_results`` option has a default of ``1000``.

        :param bucket: the bucket whose index will be queried
        :type bucket: RiakBucket
        :param index: the index to query
        :type index: string
        :param startkey: the sole key to query, or beginning of the query range
        :type startkey: string, integer
        :param endkey: the end of the query range (optional if equality)
        :type endkey: string, integer
        :param return_terms: whether to include the secondary index value
        :type return_terms: boolean
        :param max_results: the maximum number of results to return (page
            size), defaults to 1000
        :type max_results: integer
        :param continuation: the opaque continuation returned from a
            previous paginated request
        :type continuation: string
        :param timeout: a timeout value in milliseconds, or 'infinity'
        :type timeout: int
        :param term_regex: a regular expression used to filter index terms
        :type term_regex: string
        :rtype: generator over instances of
          :class:`~riak.client.index_page.IndexPage`

        """
        page = self.get_index(bucket, index, startkey,
                              endkey=endkey, max_results=max_results,
                              return_terms=return_terms,
                              continuation=continuation,
                              timeout=timeout, term_regex=term_regex)
        yield page
        while page.has_next_page():
            page = page.next_page()
            yield page

    def stream_index(self, bucket, index, startkey, endkey=None,
                     return_terms=None, max_results=None, continuation=None,
                     timeout=None, term_regex=None):
        """
        Queries a secondary index, streaming matching keys through an
        iterator.

        The caller should explicitly close the returned iterator,
        either using :func:`contextlib.closing` or calling ``close()``
        explicitly. Consuming the entire iterator will also close the
        stream. If it does not, the associated connection might not be
        returned to the pool. Example::

            from contextlib import closing

            # Using contextlib.closing
            with closing(client.stream_index(mybucket, 'name_bin',
                                             'Smith')) as index:
                for key in index:
                    do_something(key)

            # Explicit close()
            stream = client.stream_index(mybucket, 'name_bin', 'Smith')
            for key in stream:
                 do_something(key)
            stream.close()

        :param bucket: the bucket whose index will be queried
        :type bucket: RiakBucket
        :param index: the index to query
        :type index: string
        :param startkey: the sole key to query, or beginning of the query range
        :type startkey: string, integer
        :param endkey: the end of the query range (optional if equality)
        :type endkey: string, integer
        :param return_terms: whether to include the secondary index value
        :type return_terms: boolean
        :param max_results: the maximum number of results to return (page size)
        :type max_results: integer
        :param continuation: the opaque continuation returned from a
            previous paginated request
        :type continuation: string
        :param timeout: a timeout value in milliseconds, or 'infinity'
        :type timeout: int
        :param term_regex: a regular expression used to filter index terms
        :type term_regex: string
        :rtype: :class:`~riak.client.index_page.IndexPage`

        """
        # TODO FUTURE: implement "retry on connection closed"
        # as in stream_mapred
        _validate_timeout(timeout, infinity_ok=True)

        page = IndexPage(self, bucket, index, startkey, endkey,
                         return_terms, max_results, term_regex)
        page.stream = True
        resource = self._acquire()
        transport = resource.object
        page.results = transport.stream_index(
            bucket, index, startkey, endkey, return_terms=return_terms,
            max_results=max_results, continuation=continuation,
            timeout=timeout, term_regex=term_regex)
        page.results.attach(resource)
        return page

    def paginate_stream_index(self, bucket, index, startkey, endkey=None,
                              max_results=1000, return_terms=None,
                              continuation=None, timeout=None,
                              term_regex=None):
        """
        Iterates over a streaming paginated index query. This is equivalent to
        calling :meth:`stream_index` and then successively calling
        :meth:`~riak.client.index_page.IndexPage.next_page` until all
        results are exhausted.

        Because limiting the result set is necessary to invoke
        pagination, the ``max_results`` option has a default of ``1000``.

        The caller should explicitly close each yielded page, either using
        :func:`contextlib.closing` or calling ``close()`` explicitly. Consuming
        the entire page will also close the stream. If it does not, the
        associated connection might not be returned to the pool. Example::

            from contextlib import closing

            # Using contextlib.closing
            for page in client.paginate_stream_index(mybucket, 'name_bin',
                                                     'Smith'):
                with closing(page):
                    for key in page:
                        do_something(key)

            # Explicit close()
            for page in client.paginate_stream_index(mybucket, 'name_bin',
                                                     'Smith'):
                for key in page:
                    do_something(key)
                page.close()

        :param bucket: the bucket whose index will be queried
        :type bucket: RiakBucket
        :param index: the index to query
        :type index: string
        :param startkey: the sole key to query, or beginning of the query range
        :type startkey: string, integer
        :param endkey: the end of the query range (optional if equality)
        :type endkey: string, integer
        :param return_terms: whether to include the secondary index value
        :type return_terms: boolean
        :param max_results: the maximum number of results to return (page
            size), defaults to 1000
        :type max_results: integer
        :param continuation: the opaque continuation returned from a
            previous paginated request
        :type continuation: string
        :param timeout: a timeout value in milliseconds, or 'infinity'
        :type timeout: int
        :param term_regex: a regular expression used to filter index terms
        :type term_regex: string
        :rtype: generator over instances of
          :class:`~riak.client.index_page.IndexPage`

        """
        # TODO FUTURE: implement "retry on connection closed"
        # as in stream_mapred
        page = self.stream_index(bucket, index, startkey,
                                 endkey=endkey,
                                 max_results=max_results,
                                 return_terms=return_terms,
                                 continuation=continuation,
                                 timeout=timeout,
                                 term_regex=term_regex)
        yield page
        while page.has_next_page():
            page = page.next_page()
            yield page

    @retryable
    def get_bucket_props(self, transport, bucket):
        """
        get_bucket_props(bucket)

        Fetches bucket properties for the given bucket.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param bucket: the bucket whose properties will be fetched
        :type bucket: RiakBucket
        :rtype: dict
        """
        return transport.get_bucket_props(bucket)

    @retryable
    def set_bucket_props(self, transport, bucket, props):
        """
        set_bucket_props(bucket, props)

        Sets bucket properties for the given bucket.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param bucket: the bucket whose properties will be set
        :type bucket: RiakBucket
        :param props: the properties to set
        :type props: dict
        """
        _validate_bucket_props(props)
        return transport.set_bucket_props(bucket, props)

    @retryable
    def clear_bucket_props(self, transport, bucket):
        """
        clear_bucket_props(bucket)

        Resets bucket properties for the given bucket.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param bucket: the bucket whose properties will be set
        :type bucket: RiakBucket
        """
        return transport.clear_bucket_props(bucket)

    @retryable
    def get_bucket_type_props(self, transport, bucket_type):
        """
        get_bucket_type_props(bucket_type)

        Fetches properties for the given bucket-type.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param bucket_type: the bucket-type whose properties will be fetched
        :type bucket_type: BucketType
        :rtype: dict
        """
        return transport.get_bucket_type_props(bucket_type)

    @retryable
    def set_bucket_type_props(self, transport, bucket_type, props):
        """
        set_bucket_type_props(bucket_type, props)

        Sets properties for the given bucket-type.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param bucket_type: the bucket-type whose properties will be set
        :type bucket_type: BucketType
        :param props: the properties to set
        :type props: dict
        """
        _validate_bucket_props(props)
        return transport.set_bucket_type_props(bucket_type, props)

    @retryable
    def get_keys(self, transport, bucket, timeout=None):
        """
        get_keys(bucket, timeout=None)

        Lists all keys in a bucket.

        .. warning:: Do not use this in production, as it requires
           traversing through all keys stored in a cluster.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param bucket: the bucket whose keys are fetched
        :type bucket: RiakBucket
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :rtype: list
        """
        _validate_timeout(timeout)
        return transport.get_keys(bucket, timeout=timeout)

    def stream_keys(self, bucket, timeout=None):
        """
        Lists all keys in a bucket via a stream. This is a generator
        method which should be iterated over.

        .. warning:: Do not use this in production, as it requires
           traversing through all keys stored in a cluster.

        The caller should explicitly close the returned iterator,
        either using :func:`contextlib.closing` or calling ``close()``
        explicitly. Consuming the entire iterator will also close the
        stream. If it does not, the associated connection might
        not be returned to the pool. Example::

            from contextlib import closing

            # Using contextlib.closing
            with closing(client.stream_keys(mybucket)) as keys:
                for key_list in keys:
                    do_something(key_list)

            # Explicit close()
            stream = client.stream_keys(mybucket)
            for key_list in stream:
                 do_something(key_list)
            stream.close()

        :param bucket: the bucket whose properties will be set
        :type bucket: RiakBucket
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :rtype: iterator
        """
        _validate_timeout(timeout)

        def make_op(transport):
            return transport.stream_keys(bucket, timeout=timeout)

        for keylist in self._stream_with_retry(make_op):
            if len(keylist) > 0:
                if six.PY2:
                    yield keylist
                else:
                    yield [bytes_to_str(item) for item in keylist]

    @retryable
    def put(self, transport, robj, w=None, dw=None, pw=None, return_body=None,
            if_none_match=None, timeout=None):
        """
        put(robj, w=None, dw=None, pw=None, return_body=None,\
            if_none_match=None, timeout=None)

        Stores an object in the Riak cluster.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param robj: the object to store
        :type robj: RiakObject
        :param w: the write quorum
        :type w: integer, string, None
        :param dw: the durable write quorum
        :type dw: integer, string, None
        :param pw: the primary write quorum
        :type pw: integer, string, None
        :param return_body: whether to return the resulting object
           after the write
        :type return_body: boolean
        :param if_none_match: whether to fail the write if the object
          exists
        :type if_none_match: boolean
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        """
        _validate_timeout(timeout)
        return transport.put(robj, w=w, dw=dw, pw=pw,
                             return_body=return_body,
                             if_none_match=if_none_match,
                             timeout=timeout)

    @retryable
    def ts_describe(self, transport, table):
        """
        ts_describe(table)

        Retrieve a time series table description from the Riak cluster.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param table: The timeseries table.
        :type table: string or :class:`Table <riak.table.Table>`
        :rtype: :class:`TsObject <riak.ts_object.TsObject>`
        """
        t = table
        if isinstance(t, six.string_types):
            t = Table(self, table)
        return transport.ts_describe(t)

    @retryable
    def ts_get(self, transport, table, key):
        """
        ts_get(table, key)

        Retrieve timeseries value by key

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param table: The timeseries table.
        :type table: string or :class:`Table <riak.table.Table>`
        :param key: The timeseries value's key.
        :type key: list
        :rtype: :class:`TsObject <riak.ts_object.TsObject>`
        """
        t = table
        if isinstance(t, six.string_types):
            t = Table(self, table)
        return transport.ts_get(t, key)

    @retryable
    def ts_put(self, transport, tsobj):
        """
        ts_put(tsobj)

        Stores time series data in the Riak cluster.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param tsobj: the time series object to store
        :type tsobj: RiakTsObject
        :rtype: boolean
        """
        return transport.ts_put(tsobj)

    @retryable
    def ts_delete(self, transport, table, key):
        """
        ts_delete(table, key)

        Delete timeseries value by key

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param table: The timeseries table.
        :type table: string or :class:`Table <riak.table.Table>`
        :param key: The timeseries value's key.
        :type key: list or dict
        :rtype: boolean
        """
        t = table
        if isinstance(t, six.string_types):
            t = Table(self, table)
        return transport.ts_delete(t, key)

    @retryable
    def ts_query(self, transport, table, query, interpolations=None):
        """
        ts_query(table, query, interpolations=None)

        Queries time series data in the Riak cluster.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param table: The timeseries table.
        :type table: string or :class:`Table <riak.table.Table>`
        :param query: The timeseries query.
        :type query: string
        :rtype: :class:`TsObject <riak.ts_object.TsObject>`
        """
        t = table
        if isinstance(t, six.string_types):
            t = Table(self, table)
        return transport.ts_query(t, query, interpolations)

    def ts_stream_keys(self, table, timeout=None):
        """
        Lists all keys in a time series table via a stream. This is a
        generator method which should be iterated over.

        The caller should explicitly close the returned iterator,
        either using :func:`contextlib.closing` or calling ``close()``
        explicitly. Consuming the entire iterator will also close the
        stream. If it does not, the associated connection might
        not be returned to the pool. Example::

            from contextlib import closing

            # Using contextlib.closing
            with closing(client.ts_stream_keys(mytable)) as keys:
                for key_list in keys:
                    do_something(key_list)

            # Explicit close()
            stream = client.ts_stream_keys(mytable)
            for key_list in stream:
                 do_something(key_list)
            stream.close()

        :param table: the table from which to stream keys
        :type table: Table
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :rtype: iterator
        """
        _validate_timeout(timeout)
        resource = self._acquire()
        transport = resource.object
        stream = transport.ts_stream_keys(table, timeout)
        stream.attach(resource)
        try:
            for keylist in stream:
                if len(keylist) > 0:
                    yield keylist
        finally:
            stream.close()

    @retryable
    def get(self, transport, robj, r=None, pr=None, timeout=None,
            basic_quorum=None, notfound_ok=None, head_only=False):
        """
        get(robj, r=None, pr=None, timeout=None)

        Fetches the contents of a Riak object.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param robj: the object to fetch
        :type robj: RiakObject
        :param r: the read quorum
        :type r: integer, string, None
        :param pr: the primary read quorum
        :type pr: integer, string, None
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :param basic_quorum: whether to use the "basic quorum" policy
           for not-founds
        :type basic_quorum: bool
        :param notfound_ok: whether to treat not-found responses as successful
        :type notfound_ok: bool
        :param head_only: whether to fetch without value, so only metadata
           (only available on PB transport)
        :type head_only: bool
        """
        _validate_timeout(timeout)
        if not isinstance(robj.key, six.string_types):
            raise TypeError(
                'key must be a string, instead got {0}'.format(repr(robj.key)))

        return transport.get(robj, r=r, pr=pr, timeout=timeout,
                             basic_quorum=basic_quorum,
                             notfound_ok=notfound_ok,
                             head_only=head_only)

    @retryable
    def delete(self, transport, robj, rw=None, r=None, w=None, dw=None,
               pr=None, pw=None, timeout=None):
        """
        delete(robj, rw=None, r=None, w=None, dw=None, pr=None, pw=None,\
               timeout=None)

        Deletes an object from Riak.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param robj: the object to delete
        :type robj: RiakObject
        :param rw: the read/write (delete) quorum
        :type rw: integer, string, None
        :param r: the read quorum
        :type r: integer, string, None
        :param pr: the primary read quorum
        :type pr: integer, string, None
        :param w: the write quorum
        :type w: integer, string, None
        :param dw: the durable write quorum
        :type dw: integer, string, None
        :param pw: the primary write quorum
        :type pw: integer, string, None
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        """
        _validate_timeout(timeout)
        return transport.delete(robj, rw=rw, r=r, w=w, dw=dw, pr=pr,
                                pw=pw, timeout=timeout)

    @retryable
    def mapred(self, transport, inputs, query, timeout):
        """
        mapred(inputs, query, timeout)

        Executes a MapReduce query.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param inputs: the input list/structure
        :type inputs: list, dict
        :param query: the list of query phases
        :type query: list
        :param timeout: the query timeout
        :type timeout: integer, None
        :rtype: mixed
        """
        _validate_timeout(timeout)
        return transport.mapred(inputs, query, timeout)

    def stream_mapred(self, inputs, query, timeout):
        """
        Streams a MapReduce query as (phase, data) pairs. This is a
        generator method which should be iterated over.

        The caller should explicitly close the returned iterator,
        either using :func:`contextlib.closing` or calling ``close()``
        explicitly. Consuming the entire iterator will also close the
        stream. If it does not, the associated connection might
        not be returned to the pool. Example::

            from contextlib import closing

            # Using contextlib.closing
            with closing(mymapred.stream()) as results:
                for phase, result in results:
                    do_something(phase, result)

            # Explicit close()
            stream = mymapred.stream()
            for phase, result in stream:
                 do_something(phase, result)
            stream.close()

        :param inputs: the input list/structure
        :type inputs: list, dict
        :param query: the list of query phases
        :type query: list
        :param timeout: the query timeout
        :type timeout: integer, None
        :rtype: iterator
        """
        _validate_timeout(timeout)

        def make_op(transport):
            return transport.stream_mapred(inputs, query, timeout)

        for phase, data in self._stream_with_retry(make_op):
            yield phase, data

    @retryable
    def create_search_index(self, transport, index, schema=None, n_val=None,
                            timeout=None):
        """
        create_search_index(index, schema=None, n_val=None)

        Create a search index of the given name, and optionally set
        a schema. If no schema is set, the default will be used.

        :param index: the name of the index to create
        :type index: string
        :param schema: the schema that this index will follow
        :type schema: string, None
        :param n_val: this indexes N value
        :type n_val: integer, None
        :param timeout: optional timeout (in ms)
        :type timeout: integer, None
        """
        return transport.create_search_index(index, schema, n_val, timeout)

    @retryable
    def get_search_index(self, transport, index):
        """
        get_search_index(index)

        Gets a search index of the given name if it exists, which will also
        return the schema. Raises a RiakError if no such schema exists. The
        returned dict contains keys ``'name'``, ``'schema'`` and
        ``'n_val'``.

        :param index: the name of the index to create
        :type index: string
        :rtype: dict
        """
        return transport.get_search_index(index)

    @retryable
    def list_search_indexes(self, transport):
        """list_search_indexes()

        Gets all search indexes and their schemas. The returned list
        contains dicts with keys ``'name'``, ``'schema'`` and ``'n_val'``.

        :return: list of dicts
        """
        return transport.list_search_indexes()

    @retryable
    def delete_search_index(self, transport, index):
        """
        delete_search_index(index)

        Delete the search index that matches the given name.

        :param index: the name of the index to delete
        :type index: string
        """
        return transport.delete_search_index(index)

    @retryable
    def create_search_schema(self, transport, schema, content):
        """
        create_search_schema(schema, content)

        Creates a Solr schema of the given name and content.
        Content must be valid Solr schema XML.

        :param schema: the name of the schema to create
        :type schema: string
        :param content: the solr schema xml content
        :type content: string
        """
        return transport.create_search_schema(schema, content)

    @retryable
    def get_search_schema(self, transport, schema):
        """
        get_search_schema(schema)

        Gets a search schema of the given name if it exists.
        Raises a RiakError if no such schema exists.  The schema is
        returned as a dict with keys ``'name'`` and ``'content'``.

        :param schema: the name of the schema to get
        :type schema: string

        :return: dict
        """
        return transport.get_search_schema(schema)

    @retryable
    def fulltext_search(self, transport, index, query, **params):
        """
        fulltext_search(index, query, **params)

        Performs a full-text search query.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param index: the bucket/index to search over
        :type index: string
        :param query: the search query
        :type query: string
        :param params: additional query flags
        :type params: dict
        :rtype: dict
        """
        return transport.search(index, query, **params)

    @retryableHttpOnly
    def fulltext_add(self, transport, index, docs):
        """
        fulltext_add(index, docs)

        .. deprecated:: 2.1.0 (Riak 2.0)
           Manual index maintenance is not supported for
           :ref:`Riak Search 2.0 <yz-label>`.

        Adds documents to the full-text index.

        .. note:: This request is automatically retried
           :attr:`retries` times if it fails due to network error.
           Only HTTP will be used for this request.

        :param index: the bucket/index in which to index these docs
        :type index: string
        :param docs: the list of documents
        :type docs: list
        """
        transport.fulltext_add(index, docs)

    @retryableHttpOnly
    def fulltext_delete(self, transport, index, docs=None, queries=None):
        """
        fulltext_delete(index, docs=None, queries=None)

        .. deprecated:: 2.1.0 (Riak 2.0)
           Manual index maintenance is not supported for
           :ref:`Riak Search 2.0 <yz-label>`.

        Removes documents from the full-text index.

        .. note:: This request is automatically retried
           :attr:`retries` times if it fails due to network error.
           Only HTTP will be used for this request.

        :param index: the bucket/index from which to delete
        :type index: string
        :param docs: a list of documents (with ids)
        :type docs: list
        :param queries: a list of queries to match and delete
        :type queries: list
        """
        transport.fulltext_delete(index, docs, queries)

    def multiget(self, pairs, **params):
        """Fetches many keys in parallel via threads.

        :param pairs: list of bucket_type/bucket/key tuple triples
        :type pairs: list
        :param params: additional request flags, e.g. r, pr
        :type params: dict
        :rtype: list of :class:`RiakObjects <riak.riak_object.RiakObject>`,
            :class:`Datatypes <riak.datatypes.Datatype>`, or tuples of
            bucket_type, bucket, key, and the exception raised on fetch
        """
        if self._multiget_pool:
            params['pool'] = self._multiget_pool
        return riak.client.multi.multiget(self, pairs, **params)

    def multiput(self, objs, **params):
        """
        Stores objects in parallel via threads.

        :param objs: the objects to store
        :type objs: list of `RiakObject <riak.riak_object.RiakObject>`
        :param params: additional request flags, e.g. w, dw, pw
        :type params: dict
        :rtype: list of boolean or
            :class:`RiakObjects <riak.riak_object.RiakObject>`,
        """
        if self._multiput_pool:
            params['pool'] = self._multiput_pool
        return riak.client.multi.multiput(self, objs, **params)

    @retryable
    def get_counter(self, transport, bucket, key, r=None, pr=None,
                    basic_quorum=None, notfound_ok=None):
        """get_counter(bucket, key, r=None, pr=None, basic_quorum=None,\
                       notfound_ok=None)

        Gets the value of a counter.

        .. deprecated:: 2.1.0 (Riak 2.0) Riak 1.4-style counters are
           deprecated in favor of the :class:`~riak.datatypes.Counter`
           datatype.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param bucket: the bucket of the counter
        :type bucket: RiakBucket
        :param key: the key of the counter
        :type key: string
        :param r: the read quorum
        :type r: integer, string, None
        :param pr: the primary read quorum
        :type pr: integer, string, None
        :param basic_quorum: whether to use the "basic quorum" policy
           for not-founds
        :type basic_quorum: bool
        :param notfound_ok: whether to treat not-found responses as successful
        :type notfound_ok: bool
        :rtype: integer

        """
        return transport.get_counter(bucket, key, r=r, pr=pr)

    def update_counter(self, bucket, key, value, w=None, dw=None, pw=None,
                       returnvalue=False):
        """
        update_counter(bucket, key, value, w=None, dw=None, pw=None,\
                       returnvalue=False)

        .. deprecated:: 2.1.0 (Riak 2.0) Riak 1.4-style counters are
           deprecated in favor of the :class:`~riak.datatypes.Counter`
           datatype.

        Updates a counter by the given value. This operation is not
        idempotent and so should not be retried automatically.

        :param bucket: the bucket of the counter
        :type bucket: RiakBucket
        :param key: the key of the counter
        :type key: string
        :param value: the amount to increment or decrement
        :type value: integer
        :param w: the write quorum
        :type w: integer, string, None
        :param dw: the durable write quorum
        :type dw: integer, string, None
        :param pw: the primary write quorum
        :type pw: integer, string, None
        :param returnvalue: whether to return the updated value of the counter
        :type returnvalue: bool
        """
        if not isinstance(value, six.integer_types):
            raise TypeError("Counter update amount must be an integer")
        if value == 0:
            raise ValueError("Cannot increment counter by 0")

        with self._transport() as transport:
            return transport.update_counter(bucket, key, value,
                                            w=w, dw=dw, pw=pw,
                                            returnvalue=returnvalue)

    increment_counter = update_counter

    def fetch_datatype(self, bucket, key, r=None, pr=None,
                       basic_quorum=None, notfound_ok=None,
                       timeout=None, include_context=None):
        """
        Fetches the value of a Riak Datatype.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param bucket: the bucket of the datatype, which must belong to a
          :class:`~riak.bucket.BucketType`
        :type bucket: :class:`~riak.bucket.RiakBucket`
        :param key: the key of the datatype
        :type key: string
        :param r: the read quorum
        :type r: integer, string, None
        :param pr: the primary read quorum
        :type pr: integer, string, None
        :param basic_quorum: whether to use the "basic quorum" policy
           for not-founds
        :type basic_quorum: bool, None
        :param notfound_ok: whether to treat not-found responses as successful
        :type notfound_ok: bool, None
        :param timeout: a timeout value in milliseconds
        :type timeout: int, None
        :param include_context: whether to return the opaque context
          as well as the value, which is useful for removal operations
          on sets and maps
        :type include_context: bool, None
        :rtype: :class:`~riak.datatypes.Datatype`
        """
        dtype, value, context = self._fetch_datatype(
            bucket, key, r=r, pr=pr, basic_quorum=basic_quorum,
            notfound_ok=notfound_ok, timeout=timeout,
            include_context=include_context)

        return TYPES[dtype](bucket=bucket, key=key, value=value,
                            context=context)

    def update_datatype(self, datatype, w=None, dw=None, pw=None,
                        return_body=None, timeout=None,
                        include_context=None):
        """
        Sends an update to a Riak Datatype to the server. This operation is not
        idempotent and so will not be retried automatically.

        :param datatype: the datatype with pending updates
        :type datatype: :class:`~riak.datatypes.Datatype`
        :param w: the write quorum
        :type w: integer, string, None
        :param dw: the durable write quorum
        :type dw: integer, string, None
        :param pw: the primary write quorum
        :type pw: integer, string, None
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :param include_context: whether to return the opaque context
          as well as the value, which is useful for removal operations
          on sets and maps
        :type include_context: bool
        :rtype: tuple of datatype, opaque value and opaque context

        """
        _validate_timeout(timeout)

        with self._transport() as transport:
            return transport.update_datatype(datatype, w=w, dw=dw, pw=pw,
                                             return_body=return_body,
                                             timeout=timeout,
                                             include_context=include_context)

    @retryable
    def get_preflist(self, transport, bucket, key):
        """
        Fetch the preflist for a given bucket and key.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param bucket: the bucket whose index will be queried
        :type bucket: RiakBucket
        :param key: the key of the preflist
        :type key: string

        :return: list of dicts (partition, node, primary)
        """
        return transport.get_preflist(bucket, key)

    def _bucket_type_bucket_builder(self, name, bucket_type):
        """
        Build a bucket from a bucket type

        :param name: Bucket name
        :param bucket_type: A bucket type
        :return: A bucket object
        """
        return bucket_type.bucket(name)

    def _default_type_bucket_builder(self, name, unused):
        """
        Build a bucket for the default bucket type

        :param name: Default bucket name
        :param unused: Unused
        :return: A bucket object
        """
        del unused  # Ignored parameters.
        return self.bucket(name)

    @retryable
    def _fetch_datatype(self, transport, bucket, key, r=None, pr=None,
                        basic_quorum=None, notfound_ok=None,
                        timeout=None, include_context=None):
        """
        _fetch_datatype(bucket, key, r=None, pr=None, basic_quorum=None,
                       notfound_ok=None, timeout=None, include_context=None)


        Fetches the value of a Riak Datatype as raw data. This is used
        internally to update already reified Datatype objects. Use the
        public version to fetch a reified type.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param bucket: the bucket of the datatype, which must belong to a
          :class:`~riak.BucketType`
        :type bucket: RiakBucket
        :param key: the key of the datatype
        :type key: string, None
        :param r: the read quorum
        :type r: integer, string, None
        :param pr: the primary read quorum
        :type pr: integer, string, None
        :param basic_quorum: whether to use the "basic quorum" policy
           for not-founds
        :type basic_quorum: bool
        :param notfound_ok: whether to treat not-found responses as successful
        :type notfound_ok: bool
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :param include_context: whether to return the opaque context
          as well as the value, which is useful for removal operations
          on sets and maps
        :type include_context: bool
        :rtype: tuple of type, value and context
        """
        _validate_timeout(timeout)

        return transport.fetch_datatype(bucket, key, r=r, pr=pr,
                                        basic_quorum=basic_quorum,
                                        notfound_ok=notfound_ok,
                                        timeout=timeout,
                                        include_context=include_context)


def _validate_bucket_props(props):
    if 'hll_precision' in props:
        precision = props['hll_precision']
        if precision < 4 or precision > 16:
            raise ValueError(
                'hll_precision must be between 4 and 16, inclusive')


def _validate_timeout(timeout, infinity_ok=False):
    """
    Raises an exception if the given timeout is an invalid value.
    """
    if timeout is None:
        return

    if timeout == 'infinity':
        if infinity_ok:
            return
        else:
            raise ValueError(
                'timeout must be a positive integer '
                '("infinity" is not valid)')

    if isinstance(timeout, six.integer_types) and timeout > 0:
        return

    raise ValueError('timeout must be a positive integer')
