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
import urllib
from riak_object import RiakObject
from bucket import RiakBucket


class RiakMapReduce(object):
    """
    The RiakMapReduce object allows you to build up and run a
    map/reduce operation on Riak.
    """
    def __init__(self, client):
        """
        Construct a Map/Reduce object.
        @param RiakClient client - A RiakClient object.
        @return RiakMapReduce
        """
        self._client = client
        self._phases = []
        self._inputs = []
        self._key_filters = []
        self._input_mode = None

    def add(self, arg1, arg2=None, arg3=None):
        """
        Add inputs to a map/reduce operation. This method takes three
        different forms, depending on the provided inputs. You can
        specify either a RiakObject, a string bucket name, or a bucket,
        key, and additional arg.
        @param mixed arg1 - RiakObject or Bucket
        @param mixed arg2 - Key or blank
        @param mixed arg3 - Arg or blank
        @return RiakMapReduce
        """
        if (arg2 is None) and (arg3 is None):
            if isinstance(arg1, RiakObject):
                return self.add_object(arg1)
            else:
                return self.add_bucket(arg1)
        else:
            return self.add_bucket_key_data(arg1, arg2, arg3)

    def add_object(self, obj):
        return self.add_bucket_key_data(obj._bucket._name, obj._key, None)

    def add_bucket_key_data(self, bucket, key, data):
        if self._input_mode == 'bucket':
            raise Exception('Already added a bucket, can\'t add an object.')
        elif self._input_mode == 'query':
            raise Exception('Already added a query, can\'t add an object.')
        else:
            self._inputs.append([bucket, key, data])
            return self

    def add_bucket(self, bucket):
        self._input_mode = 'bucket'
        self._inputs = bucket
        return self

    def add_key_filters(self, key_filters):
        if self._input_mode == 'query':
            raise Exception('Key filters are not supported in a query.')

        self._key_filters.extend(key_filters)
        return self

    def add_key_filter(self, *args):
        if self._input_mode == 'query':
            raise Exception('Key filters are not supported in a query.')

        self._key_filters.append(args)
        return self

    def search(self, bucket, query):
        """
        Begin a map/reduce operation using a Search. This command will
        return an error unless executed against a Riak Search cluster.
        @param bucket - The bucket over which to perform the search.
        @param query - The search query.
        """
        self._input_mode = 'query'
        self._inputs = {'module': 'riak_search',
                        'function': 'mapred_search',
                        'arg': [bucket, query]}
        return self

    def index(self, bucket, index, startkey, endkey=None):
        """
        Begin a map/reduce operation using a Secondary Index
        query.
        @param bucket - The bucket over which to perform the search.
        @param index - The index to use for query
        @param startkey - The start key of index range
        @param endkey - The end key of index range or blank
        """
        self._input_mode = 'query'

        if endkey == None:
            self._inputs = {'bucket': bucket,
                            'index': index,
                            'key': startkey}
        else:
            self._inputs = {'bucket': bucket,
                            'index': index,
                            'start': startkey,
                            'end': endkey}
        return self

    def link(self, bucket='_', tag='_', keep=False):
        """
        Add a link phase to the map/reduce operation.
        @param string bucket - Bucket name (default '_', which means all
        buckets)
        @param string tag - Tag (default '_', which means any tag)
        @param boolean keep - Flag whether to keep results from this
        stage in the map/reduce. (default False, unless this is the last
        step in the phase)
        @return self
        """
        self._phases.append(RiakLinkPhase(bucket, tag, keep))
        return self

    def map(self, function, options=None):
        """
        Add a map phase to the map/reduce operation.
        @param mixed function - Either a named Javascript function (ie:
        'Riak.mapValues'), or an anonymous javascript function (ie:
        'function(...)  ... ' or an array ['erlang_module',
        'function'].
        @param array() options - An optional associative array
        containing 'language', 'keep' flag, and/or 'arg'.
        @return self
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
        @param mixed function - Either a named Javascript function (ie.
        'Riak.mapValues'), or an anonymous javascript function(ie:
        'function(...) { ... }' or an array ['erlang_module', 'function'].
        @param array() options - An optional associative array
        containing 'language', 'keep' flag, and/or 'arg'.
        @return self
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
        Run the map/reduce operation. Returns an array of results, or an
        array of RiakLink objects if the last phase is a link phase.
        @param integer timeout - Timeout in milliseconds.
        @return array()
        """
        num_phases = len(self._phases)

        # If there are no phases, then just echo the inputs back to the user.
        if (num_phases == 0):
            self.reduce(["riak_kv_mapreduce", "reduce_identity"])
            num_phases = 1
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

        if (len(self._key_filters) > 0):
            bucket_name = None
            if (type(self._inputs) == str):
                bucket_name = self._inputs
            elif (type(self._inputs) == RiakBucket):
                bucket_name = self._inputs.get_name()

            if (bucket_name is not None):
                self._inputs = {'bucket':       bucket_name,
                                'key_filters':  self._key_filters}

        t = self._client.get_transport()
        result = t.mapred(self._inputs, query, timeout)

        # If the last phase is NOT a link phase, then return the result.
        if not (link_results_flag
                or isinstance(self._phases[-1], RiakLinkPhase)):
            return result

        # If there are no results, then return an empty list.
        if result == None:
            return []

        # Otherwise, if the last phase IS a link phase, then convert the
        # results to RiakLink objects.
        a = []
        for r in result:
            if (len(r) == 2):
                link = RiakLink(r[0], r[1])
            elif (len(r) == 3):
                link = RiakLink(r[0], r[1], r[2])
            link._client = self._client
            a.append(link)

        return a

    ##
    # Start Shortcuts to built-ins
    ##
    def map_values(self, options=None):
        return self.map("Riak.mapValues", options=options)

    def map_values_json(self, options=None):
        return self.map("Riak.mapValuesJson", options=options)

    def reduce_sum(self, options=None):
        return self.reduce("Riak.reduceSum", options=options)

    def reduce_min(self, options=None):
        return self.reduce("Riak.reduceMin", options=options)

    def reduce_max(self, options=None):
        return self.reduce("Riak.reduceMax", options=options)

    def reduce_sort(self, js_cmp=None, options=None):
        if options is None:
            options = dict()

        if js_cmp:
            options['arg'] = js_cmp

        return self.reduce("Riak.reduceSort", options=options)

    def reduce_numeric_sort(self, options=None):
        return self.reduce("Riak.reduceNumericSort", options=options)

    def reduce_limit(self, limit, options=None):
        if options is None:
            options = dict()

        options['arg'] = limit
        # reduceLimit is broken in riak_kv
        code = """function(value, arg) {
            return value.slice(0, arg);
        }"""
        return self.reduce(code, options=options)

    def reduce_slice(self, start, end, options=None):
        if options is None:
            options = dict()

        options['arg'] = [start, end]
        return self.reduce("Riak.reduceSlice", options=options)

    def filter_not_found(self, options=None):
        return self.reduce("Riak.filterNotFound", options=options)


class RiakMapReducePhase(object):
    """
    The RiakMapReducePhase holds information about a Map phase or
    Reduce phase in a RiakMapReduce operation.
    """

    def __init__(self, type, function, language, keep, arg):
        """
        Construct a RiakMapReducePhase object.
        @param string type - 'map'placeholder149'reduce'
        @param mixed function - string or array():
        @param string language - 'javascript'placeholder149'erlang'
        @param boolean keep - True to return the output of this phase in
        the results.
        @param mixed arg - Additional value to pass into the map or
        reduce function.
        """
        try:
            if isinstance(function, basestring):
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
        Convert the RiakMapReducePhase to an associative array. Used
        internally.
        """
        stepdef = {'keep': self._keep,
                   'language': self._language,
                   'arg': self._arg}

        if self._language == 'javascript':
            if isinstance(self._function, list):
                stepdef['bucket'] = self._function[0]
                stepdef['key'] = self._function[1]
            elif isinstance(self._function, str):
                if ("{" in self._function):
                    stepdef['source'] = self._function
                else:
                    stepdef['name'] = self._function

        elif (self._language == 'erlang' and isinstance(self._function, list)):
            stepdef['module'] = self._function[0]
            stepdef['function'] = self._function[1]

        return {self._type: stepdef}


class RiakLinkPhase(object):
    """
    The RiakLinkPhase object holds information about a Link phase in a
    map/reduce operation.
    """

    def __init__(self, bucket, tag, keep):
        """
        Construct a RiakLinkPhase object.
        @param string bucket - The bucket name.
        @param string tag - The tag.
        @param boolean keep - True to return results of this phase.
        """
        self._bucket = bucket
        self._tag = tag
        self._keep = keep

    def to_array(self):
        """
        Convert the RiakLinkPhase to an associative array. Used
        internally.
        """
        stepdef = {'bucket': self._bucket,
                   'tag': self._tag,
                   'keep': self._keep}
        return {'link': stepdef}


class RiakLink(object):
    """
    The RiakLink object represents a link from one Riak object to
    another.
    """

    def __init__(self, bucket, key, tag=None):
        """
        Construct a RiakLink object.
        @param string bucket - The bucket name.
        @param string key - The key.
        @param string tag - The tag.
        """
        self._bucket = bucket
        self._key = key
        self._tag = tag
        self._client = None

    def get(self, r=None):
        """
        Retrieve the RiakObject to which this link points.
        @param integer r - The R-value to use.
        @return RiakObject
        """
        return self._client.bucket(self._bucket).get(self._key, r)

    def get_binary(self, r=None):
        """
        Retrieve the RiakObject to which this link points, as a binary.
        @param integer r - The R-value to use.
        @return RiakObject
        """
        return self._client.bucket(self._bucket).get_binary(self._key, r)

    def get_bucket(self):
        """
        Get the bucket name of this link.
        @return string
        """
        return self._bucket

    def set_bucket(self, name):
        """
        Set the bucket name of this link.
        @param string name - The bucket name.
        @return self
        """
        self._bucket = name
        return self

    def get_key(self):
        """
        Get the key of this link.
        @return string
        """
        return self._key

    def set_key(self, key):
        """
        Set the key of this link.
        @param string key - The key.
        @return self
        """
        self._key = key
        return self

    def get_tag(self):
        """
        Get the tag of this link.
        @return string
        """
        if (self._tag is None):
            return self._bucket
        else:
            return self._tag

    def set_tag(self, tag):
        """
        Set the tag of this link.
        @param string tag - The tag.
        @return self
        """
        self._tag = tag
        return self

    def to_link_header(self, client):
        """
        Convert this RiakLink object to a link header string. Used internally.
        """
        link = ''
        link += '</'
        link += client._prefix + '/'
        link += urllib.quote_plus(self._bucket) + '/'
        link += urllib.quote_plus(self._key) + '>; riaktag="'
        link += urllib.quote_plus(self.get_tag()) + '"'
        return link

    def isEqual(self, link):
        """
        Return True if the links are equal.
        @param RiakLink link - A RiakLink object.
        @return boolean
        """

        return ((self._bucket == link._bucket) and
                (self._key == link._key) and
                (self.get_tag() == link.get_tag()))


class RiakKeyFilter(object):
    def __init__(self, *args):
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
