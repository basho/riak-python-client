"""
Copyright 2010 Rusty Klophaus <rusty@basho.com>
Copyright 2010 Justin Sheehy <justin@basho.com>
Copyright 2009 Jay Baird <jay@mochimedia.com>

This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from __future__ import print_function
from collections import Iterable, namedtuple
from riak import RiakError
from six import string_types, PY2
from riak.bucket import RiakBucket


#: Links are just bucket/key/tag tuples, this class provides a
#: backwards-compatible format: ``RiakLink(bucket, key, tag)``
RiakLink = namedtuple("RiakLink", ("bucket", "key", "tag"))


class RiakMapReduce(object):
    """
    The RiakMapReduce object allows you to build up and run a
    map/reduce operation on Riak. Most methods return the object on
    which it was called, modified with new information, so you can
    chain calls together to build the job.
    """
    def __init__(self, client):
        """
        Construct a Map/Reduce object.

        :param client: the client that will perform the query
        :type client: :class:`~riak.client.RiakClient`
        """
        self._client = client
        self._phases = []
        self._inputs = []
        self._key_filters = []
        self._input_mode = None

    def add(self, arg1, arg2=None, arg3=None, bucket_type=None):
        """
        Add inputs to a map/reduce operation. This method takes three
        different forms, depending on the provided inputs. You can
        specify either a RiakObject, a string bucket name, or a bucket,
        key, and additional arg.

        :param arg1: the object or bucket to add
        :type arg1: RiakObject, string
        :param arg2: a key or list of keys to add (if a bucket is
          given in arg1)
        :type arg2: string, list, None
        :param arg3: key data for this input (must be convertible to JSON)
        :type arg3: string, list, dict, None
        :param bucket_type: Optional name of a bucket type
        :type bucket_type: string, None
        :rtype: :class:`RiakMapReduce`
        """
        from riak.riak_object import RiakObject
        if (arg2 is None) and (arg3 is None):
            if isinstance(arg1, RiakObject):
                return self.add_object(arg1)
            else:
                return self.add_bucket(arg1, bucket_type)
        else:
            return self.add_bucket_key_data(arg1, arg2, arg3, bucket_type)

    def add_object(self, obj):
        """
        Adds a RiakObject to the inputs.

        :param obj: the object to add
        :type obj: RiakObject
        :rtype: :class:`RiakMapReduce`
        """
        return self.add_bucket_key_data(obj._bucket._name, obj._key, None)

    def add_bucket_key_data(self, bucket, key, data, bucket_type=None):
        """
        Adds a bucket/key/keydata triple to the inputs.

        :param bucket: the bucket
        :type bucket: string
        :param key: the key or list of keys
        :type key: string
        :param data: the key-specific data
        :type data: string, list, dict, None
        :param bucket_type: Optional name of a bucket type
        :type bucket_type: string, None
        :rtype: :class:`RiakMapReduce`
        """
        if self._input_mode == 'bucket':
            raise ValueError('Already added a bucket, can\'t add an object.')
        elif self._input_mode == 'query':
            raise ValueError('Already added a query, can\'t add an object.')
        else:
            if isinstance(key, Iterable) and \
                    not isinstance(key, string_types):
                if bucket_type is not None:
                    for k in key:
                        self._inputs.append([bucket, k, data, bucket_type])
                else:
                    for k in key:
                        self._inputs.append([bucket, k, data])
            else:
                if bucket_type is not None:
                    self._inputs.append([bucket, key, data, bucket_type])
                else:
                    self._inputs.append([bucket, key, data])
            return self

    def add_bucket(self, bucket, bucket_type=None):
        """
        Adds all keys in a bucket to the inputs.

        :param bucket: the bucket
        :type bucket: string
        :param bucket_type: Optional name of a bucket type
        :type bucket_type: string, None
        :rtype: :class:`RiakMapReduce`
        """
        self._input_mode = 'bucket'
        if isinstance(bucket, RiakBucket):
            if bucket.bucket_type.is_default():
                self._inputs = {'bucket': bucket.name}
            else:
                self._inputs = {'bucket': [bucket.bucket_type.name,
                                           bucket.name]}
        elif bucket_type is not None and bucket_type != "default":
            self._inputs = {'bucket': [bucket_type, bucket]}
        else:
            self._inputs = {'bucket': bucket}
        return self

    def add_key_filters(self, key_filters):
        """
        Adds key filters to the inputs.

        :param key_filters: a list of filters
        :type key_filters: list
        :rtype: :class:`RiakMapReduce`
        """
        if self._input_mode == 'query':
            raise ValueError('Key filters are not supported in a query.')

        self._key_filters.extend(key_filters)
        return self

    def add_key_filter(self, *args):
        """
        Add a single key filter to the inputs.

        :param args: a filter
        :type args: list
        :rtype: :class:`RiakMapReduce`
        """
        if self._input_mode == 'query':
            raise ValueError('Key filters are not supported in a query.')

        self._key_filters.append(args)
        return self

    def search(self, index, query):
        """
        Begin a map/reduce operation using a Search. This command will
        return an error unless executed against a Riak Search cluster.

        :param index: The Solr index used in the search
        :type index: string
        :param query: The search query
        :type query: string
        :rtype: :class:`RiakMapReduce`
        """
        self._input_mode = 'query'
        self._inputs = {'bucket': index,
                        'index': index,
                        'query': query}
        return self

    def index(self, bucket, index, startkey, endkey=None, bucket_type=None):
        """
        Begin a map/reduce operation using a Secondary Index
        query.

        :param bucket: The bucket over which to perform the query
        :type bucket: string
        :param index: The index to use for query
        :type index: string
        :param startkey: The start key of index range, or the
           value which all entries must equal
        :type startkey: string, integer
        :param endkey: The end key of index range (if doing a range query)
        :type endkey: string, integer, None
        :param bucket_type: Optional name of a bucket type
        :type bucket_type: string, None
        :rtype: :class:`RiakMapReduce`
        """
        self._input_mode = 'query'

        if endkey is None:
            self._inputs = {'bucket': bucket,
                            'index': index,
                            'key': startkey}
        else:
            self._inputs = {'bucket': bucket,
                            'index': index,
                            'start': startkey,
                            'end': endkey}
        if bucket_type is not None:
            self._inputs['bucket'] = [bucket_type, bucket]
        return self

    def link(self, bucket='_', tag='_', keep=False):
        """
        Add a link phase to the map/reduce operation.

        :param bucket: Bucket name (default '_', which means all
            buckets)
        :type bucket: string
        :param tag:  Tag (default '_', which means any tag)
        :type tag: string
        :param keep: Flag whether to keep results from this stage in
          the map/reduce. (default False, unless this is the last step
          in the phase)
        :type keep: boolean
        :rtype: :class:`RiakMapReduce`
        """
        self._phases.append(RiakLinkPhase(bucket, tag, keep))
        return self

    def map(self, function, options=None):
        """
        Add a map phase to the map/reduce operation.

        :param function: Either a named Javascript function (ie:
          'Riak.mapValues'), or an anonymous javascript function (ie:
          'function(...) ... ' or an array ['erlang_module',
          'function'].
        :type function: string, list
        :param options: phase options, containing 'language', 'keep'
          flag, and/or 'arg'.
        :type options: dict
        :rtype: :class:`RiakMapReduce`
        """
        if options is None:
            options = dict()
        if isinstance(function, list):
            language = 'erlang'
        else:
            language = 'javascript'

        mr = RiakMapReducePhase('map',
                                function,
                                options.get('language', language),
                                options.get('keep', False),
                                options.get('arg', None))
        self._phases.append(mr)
        return self

    def reduce(self, function, options=None):
        """
        Add a reduce phase to the map/reduce operation.

        :param function: Either a named Javascript function (ie.
          'Riak.reduceSum'), or an anonymous javascript function(ie:
          'function(...) { ... }' or an array ['erlang_module',
          'function'].
        :type function: string, list
        :param options: phase options, containing 'language', 'keep'
          flag, and/or 'arg'.
        :rtype: :class:`RiakMapReduce`
        """
        if options is None:
            options = dict()
        if isinstance(function, list):
            language = 'erlang'
        else:
            language = 'javascript'

        mr = RiakMapReducePhase('reduce',
                                function,
                                options.get('language', language),
                                options.get('keep', False),
                                options.get('arg', None))
        self._phases.append(mr)
        return self

    def run(self, timeout=None):
        """
        Run the map/reduce operation synchronously. Returns a list of
        results, or a list of links if the last phase is a link phase.
        Shortcut for :meth:`riak.client.RiakClient.mapred`.

        :param timeout: Timeout in milliseconds
        :type timeout: integer, None
        :rtype: list
        """
        query, link_results_flag = self._normalize_query()

        try:
            result = self._client.mapred(self._inputs, query, timeout)
        except RiakError as e:
            if 'worker_startup_failed' in e.value:
                for phase in self._phases:
                    if phase._language == 'erlang':
                        if type(phase._function) is str:
                            raise RiakError('May have tried erlang strfun '
                                            'when not allowed\n'
                                            'original error: ' + e.value)
            raise e

        # If the last phase is NOT a link phase, then return the result.
        if not (link_results_flag or
                isinstance(self._phases[-1], RiakLinkPhase)):
            return result

        # If there are no results, then return an empty list.
        if result is None:
            return []

        # Otherwise, if the last phase IS a link phase, then convert the
        # results to link tuples.
        a = []
        for r in result:
            if (len(r) == 2):
                link = (r[0], r[1], None)
            elif (len(r) == 3):
                link = (r[0], r[1], r[2])
            a.append(link)

        return a

    def stream(self, timeout=None):
        """
        Streams the MapReduce query (returns an iterator). Shortcut
        for :meth:`riak.client.RiakClient.stream_mapred`.

        :param timeout: Timeout in milliseconds
        :type timeout: integer
        :rtype: iterator that yields (phase_num, data) tuples
        """
        query, lrf = self._normalize_query()
        return self._client.stream_mapred(self._inputs, query, timeout)

    def _normalize_query(self):
        num_phases = len(self._phases)

        # If there are no phases, return the keys as links
        if num_phases is 0:
            link_results_flag = True
        else:
            link_results_flag = False

        # Convert all phases to associative arrays. Also,
        # if none of the phases are accumulating, then set the last one to
        # accumulate.
        keep_flag = False
        query = []
        for i in range(num_phases):
            phase = self._phases[i]
            if (i == (num_phases - 1)) and (not keep_flag):
                phase._keep = True
            if phase._keep:
                keep_flag = True
            query.append(phase.to_array())

        # Is this a bucket-only input? If so, we need to switch
        # to be a string or array, i.e., no keyword (to keep Riak happy)
        # Also add the key filter, if necessary
        if isinstance(self._inputs, dict) and len(self._inputs) == 1:
            if len(self._key_filters) > 0:
                self._inputs['key_filters'] = self._key_filters
            else:
                self._inputs = self._inputs['bucket']

        return query, link_results_flag

    ##
    # Start Shortcuts to built-ins
    ##
    def map_values(self, options=None):
        """
        Adds the Javascript built-in ``Riak.mapValues`` to the query
        as a map phase.

        :param options: phase options, containing 'language', 'keep'
          flag, and/or 'arg'.
        :type options: dict
        """
        return self.map("Riak.mapValues", options=options)

    def map_values_json(self, options=None):
        """
        Adds the Javascript built-in ``Riak.mapValuesJson`` to the
        query as a map phase.

        :param options: phase options, containing 'language', 'keep'
          flag, and/or 'arg'.
        :type options: dict
        """
        return self.map("Riak.mapValuesJson", options=options)

    def reduce_sum(self, options=None):
        """
        Adds the Javascript built-in ``Riak.reduceSum`` to the query
        as a reduce phase.

        :param options: phase options, containing 'language', 'keep'
          flag, and/or 'arg'.
        :type options: dict
        """
        return self.reduce("Riak.reduceSum", options=options)

    def reduce_min(self, options=None):
        """
        Adds the Javascript built-in ``Riak.reduceMin`` to the query
        as a reduce phase.

        :param options: phase options, containing 'language', 'keep'
          flag, and/or 'arg'.
        :type options: dict
        """
        return self.reduce("Riak.reduceMin", options=options)

    def reduce_max(self, options=None):
        """
        Adds the Javascript built-in ``Riak.reduceMax`` to the query
        as a reduce phase.

        :param options: phase options, containing 'language', 'keep'
          flag, and/or 'arg'.
        :type options: dict
        """
        return self.reduce("Riak.reduceMax", options=options)

    def reduce_sort(self, js_cmp=None, options=None):
        """
        Adds the Javascript built-in ``Riak.reduceSort`` to the query
        as a reduce phase.

        :param js_cmp: A Javascript comparator function as specified by
          Array.sort()
        :type js_cmp: string
        :param options: phase options, containing 'language', 'keep'
          flag, and/or 'arg'.
        :type options: dict
        """
        if options is None:
            options = dict()

        if js_cmp:
            options['arg'] = js_cmp

        return self.reduce("Riak.reduceSort", options=options)

    def reduce_numeric_sort(self, options=None):
        """
        Adds the Javascript built-in ``Riak.reduceNumericSort`` to the
        query as a reduce phase.

        :param options: phase options, containing 'language', 'keep'
          flag, and/or 'arg'.
        :type options: dict
        """
        return self.reduce("Riak.reduceNumericSort", options=options)

    def reduce_limit(self, limit, options=None):
        """
        Adds the Javascript built-in ``Riak.reduceLimit`` to the query
        as a reduce phase.

        :param limit: the maximum number of results to return
        :type limit: integer
        :param options: phase options, containing 'language', 'keep'
          flag, and/or 'arg'.
        :type options: dict
        """
        if options is None:
            options = dict()

        options['arg'] = limit
        # reduceLimit is broken in riak_kv
        code = """function(value, arg) {
            return value.slice(0, arg);
        }"""
        return self.reduce(code, options=options)

    def reduce_slice(self, start, end, options=None):
        """
        Adds the Javascript built-in ``Riak.reduceSlice`` to the
        query as a reduce phase.

        :param start: the beginning of the slice
        :type start: integer
        :param end: the end of the slice
        :type end: integer
        :param options: phase options, containing 'language', 'keep'
          flag, and/or 'arg'.
        :type options: dict
        """
        if options is None:
            options = dict()

        options['arg'] = [start, end]
        return self.reduce("Riak.reduceSlice", options=options)

    def filter_not_found(self, options=None):
        """
        Adds the Javascript built-in ``Riak.filterNotFound`` to the query
        as a reduce phase.

        :param options: phase options, containing 'language', 'keep'
          flag, and/or 'arg'.
        :type options: dict
        """
        return self.reduce("Riak.filterNotFound", options=options)


class RiakMapReducePhase(object):
    """
    The RiakMapReducePhase holds information about a Map or Reduce
    phase in a RiakMapReduce operation.

    Normally you won't need to use this object directly, but instead
    call methods on RiakMapReduce objects to add instances to the
    query.
    """

    def __init__(self, type, function, language, keep, arg):
        """
        Construct a RiakMapReducePhase object.

        :param type: the phase type - 'map', 'reduce', 'link'
        :type type: string
        :param function: the function to execute
        :type function: string, list
        :param language: 'javascript' or 'erlang'
        :type language: string
        :param keep: whether to return the output of this phase in the results.
        :type keep: boolean
        :param arg: Additional static value to pass into the map or
          reduce function.
        :type arg: string, dict, list
        """
        try:
            if isinstance(function, string_types) and PY2:
                function = function.encode('ascii')
        except UnicodeError:
            raise TypeError('Unicode encoded functions are not supported.')

        self._type = type
        self._language = language
        self._function = function
        self._keep = keep
        self._arg = arg

    def to_array(self):
        """
        Convert the RiakMapReducePhase to a format that can be output
        into JSON. Used internally.

        :rtype: dict
        """
        stepdef = {'keep': self._keep,
                   'language': self._language,
                   'arg': self._arg}

        if self._language == 'javascript':
            if isinstance(self._function, list):
                stepdef['bucket'] = self._function[0]
                stepdef['key'] = self._function[1]
            elif isinstance(self._function, string_types):
                if ("{" in self._function):
                    stepdef['source'] = self._function
                else:
                    stepdef['name'] = self._function

        elif (self._language == 'erlang' and isinstance(self._function, list)):
            stepdef['module'] = self._function[0]
            stepdef['function'] = self._function[1]

        elif (self._language == 'erlang' and
              isinstance(self._function, string_types)):
            stepdef['source'] = self._function

        return {self._type: stepdef}


class RiakLinkPhase(object):
    """
    The RiakLinkPhase object holds information about a Link phase in a
    map/reduce operation.

    Normally you won't need to use this object directly, but instead
    call :meth:`RiakMapReduce.link` on RiakMapReduce objects to add
    instances to the query.
    """

    def __init__(self, bucket, tag, keep):
        """
        Construct a RiakLinkPhase object.

        :param bucket: - The bucket name
        :type bucket: string
        :param tag: The tag
        :type tag: string
        :param keep: whether to return results of this phase.
        :type keep: boolean
        """
        self._bucket = bucket
        self._tag = tag
        self._keep = keep

    def to_array(self):
        """
        Convert the RiakLinkPhase to a format that can be output into
        JSON. Used internally.
        """
        stepdef = {'bucket': self._bucket,
                   'tag': self._tag,
                   'keep': self._keep}
        return {'link': stepdef}


class RiakKeyFilter(object):
    """
    A helper class for building up lists of key filters. Unknown
    methods are treated as filters to be added; ``&`` and ``|`` create
    conjunctions and disjunctions, respectively. ``+`` concatenates filters.

    Example::

        f1 = RiakKeyFilter().starts_with('2005')
        f2 = RiakKeyFilter().ends_with('-01')
        f3 = f1 & f2
        print(f3)
        # => [['and', [['starts_with', '2005']], [['ends_with', '-01']]]]
    """

    def __init__(self, *args):
        """
        :param args: a list of arguments to be treated as a filter.
        :type args: list
        """
        if args:
            self._filters = [list(args)]
        else:
            self._filters = []

    def __add__(self, other):
        f = RiakKeyFilter()
        f._filters = self._filters + other._filters
        return f

    def _bool_op(self, op, other):
        # If the current filter is an and, append the other's
        # filters onto the filter
        if(self._filters and self._filters[0][0] == op):
            f = RiakKeyFilter()
            f._filters.extend(self._filters)
            f._filters[0].append(other._filters)
            return f
        # Otherwise just create a new RiakKeyFilter() object with an and
        return RiakKeyFilter(op, self._filters, other._filters)

    def __and__(self, other):
        return self._bool_op("and", other)

    def __or__(self, other):
        return self._bool_op("or", other)

    def __repr__(self):
        return str(self._filters)

    def __getattr__(self, name):
        def function(*args):
            args1 = [name] + list(args)
            other = RiakKeyFilter(*args1)
            return self + other
        return function

    def __iter__(self):
        return iter(self._filters)


class RiakMapReduceChain(object):
    """
    Mixin to add chaining from the client object directly into a
    MapReduce operation.
    """
    def add(self, arg1, arg2=None, arg3=None, bucket_type=None):
        """
        Start assembling a Map/Reduce operation. A shortcut for
        :func:`RiakMapReduce.add`.

        :param arg1: the object or bucket to add
        :type arg1: RiakObject, string
        :param arg2: a key or list of keys to add (if a bucket is
          given in arg1)
        :type arg2: string, list, None
        :param arg3: key data for this input (must be convertible to JSON)
        :type arg3: string, list, dict, None
        :param bucket_type: Optional name of a bucket type
        :type bucket_type: string, None
        :rtype: :class:`RiakMapReduce`
        """
        mr = RiakMapReduce(self)
        return mr.add(arg1, arg2, arg3, bucket_type)

    def search(self, *args):
        """
        Start assembling a Map/Reduce operation based on search
        results. This command will return an error unless executed
        against a Riak Search cluster. A shortcut for
        :func:`RiakMapReduce.search`.

        :rtype: :class:`RiakMapReduce`
        """

        mr = RiakMapReduce(self)
        return mr.search(*args)

    def index(self, bucket, index, startkey, endkey=None, bucket_type=None):
        """
        Start assembling a Map/Reduce operation based on secondary
        index query results.

        :param bucket: The bucket over which to perform the query
        :type bucket: string
        :param index: The index to use for query
        :type index: string
        :param startkey: The start key of index range, or the
           value which all entries must equal
        :type startkey: string, integer
        :param endkey: The end key of index range (if doing a range query)
        :type endkey: string, integer, None
        :param bucket_type: Optional name of a bucket type
        :type bucket_type: string, None
        :rtype: :class:`RiakMapReduce`
        """

        mr = RiakMapReduce(self)
        return mr.index(bucket, index, startkey, endkey, bucket_type)

    def link(self, *args):
        """
        Start assembling a Map/Reduce operation. A shortcut for
        :func:`RiakMapReduce.link`.

        :rtype: :class:`RiakMapReduce`
        """
        mr = RiakMapReduce(self)
        return mr.link(*args)

    def map(self, *args):
        """
        Start assembling a Map/Reduce operation. A shortcut for
        :func:`RiakMapReduce.map`.

        :rtype: :class:`RiakMapReduce`
        """
        mr = RiakMapReduce(self)
        return mr.map(*args)

    def reduce(self, *args):
        """
        Start assembling a Map/Reduce operation. A shortcut for
        :func:`RiakMapReduce.reduce`.

        :rtype: :class:`RiakMapReduce`
        """
        mr = RiakMapReduce(self)
        return mr.reduce(*args)
