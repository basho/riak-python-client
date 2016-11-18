import base64
import random
import threading
import os
import json
import platform

from six import PY2
from riak.transports.feature_detect import FeatureDetection


class Transport(FeatureDetection):
    """
    Class to encapsulate transport details and methods. All protocol
    transports are subclasses of this class.
    """

    def _get_client_id(self):
        return self._client_id

    def _set_client_id(self, value):
        self._client_id = value

    client_id = property(_get_client_id, _set_client_id,
                         doc="""the client ID for this connection""")

    @classmethod
    def make_random_client_id(self):
        """
        Returns a random client identifier
        """
        if PY2:
            return ('py_%s' %
                    base64.b64encode(str(random.randint(1, 0x40000000))))
        else:
            return ('py_%s' %
                    base64.b64encode(bytes(str(random.randint(1, 0x40000000)),
                                     'ascii')))

    @classmethod
    def make_fixed_client_id(self):
        """
        Returns a unique identifier for the current machine/process/thread.
        """
        machine = platform.node()
        process = os.getpid()
        thread = threading.currentThread().getName()
        return base64.b64encode('%s|%s|%s' % (machine, process, thread))

    def ping(self):
        """
        Ping the remote server
        """
        raise NotImplementedError

    def get(self, robj, r=None, pr=None, timeout=None, basic_quorum=None,
            notfound_ok=None, head_only=False):
        """
        Fetches an object.
        """
        raise NotImplementedError

    def put(self, robj, w=None, dw=None, pw=None, return_body=None,
            if_none_match=None, timeout=None):
        """
        Stores an object.
        """
        raise NotImplementedError

    def delete(self, robj, rw=None, r=None, w=None, dw=None, pr=None,
               pw=None, timeout=None):
        """
        Deletes an object.
        """
        raise NotImplementedError

    def ts_describe(self, table):
        """
        Retrieves a timeseries table description.
        """
        raise NotImplementedError

    def ts_get(self, table, key):
        """
        Retrieves a timeseries object.
        """
        raise NotImplementedError

    def ts_put(self, tsobj):
        """
        Stores a timeseries object.
        """
        raise NotImplementedError

    def ts_delete(self, table, key):
        """
        Deletes a timeseries object.
        """
        raise NotImplementedError

    def ts_query(self, table, query, interpolations=None):
        """
        Query timeseries data.
        """
        raise NotImplementedError

    def ts_stream_keys(self, table, timeout=None):
        """
        Streams the list of keys for the table through an iterator.
        """
        raise NotImplementedError

    def get_buckets(self, bucket_type=None, timeout=None):
        """
        Gets the list of buckets as strings.
        """
        raise NotImplementedError

    def stream_buckets(self, bucket_type=None, timeout=None):
        """
        Streams the list of buckets through an iterator
        """
        raise NotImplementedError

    def get_bucket_props(self, bucket):
        """
        Fetches properties for the given bucket.
        """
        raise NotImplementedError

    def set_bucket_props(self, bucket, props):
        """
        Sets properties on the given bucket.
        """
        raise NotImplementedError

    def get_bucket_type_props(self, bucket_type):
        """
        Fetches properties for the given bucket-type.
        """
        raise NotImplementedError

    def set_bucket_type_props(self, bucket_type, props):
        """
        Sets properties on the given bucket-type.
        """
        raise NotImplementedError

    def clear_bucket_props(self, bucket):
        """
        Reset bucket properties to their defaults
        """
        raise NotImplementedError

    def get_keys(self, bucket, timeout=None):
        """
        Lists all keys within the given bucket.
        """
        raise NotImplementedError

    def stream_keys(self, bucket, timeout=None):
        """
        Streams the list of keys for the bucket through an iterator.
        """
        raise NotImplementedError

    def mapred(self, inputs, query, timeout=None):
        """
        Sends a MapReduce request synchronously.
        """
        raise NotImplementedError

    def stream_mapred(self, inputs, query, timeout=None):
        """
        Streams the results of a MapReduce request through an iterator.
        """
        raise NotImplementedError

    def set_client_id(self, client_id):
        """
        Set the client id. This overrides the default, random client
        id, which is automatically generated when none is specified in
        when creating the transport object.
        """
        raise NotImplementedError

    def get_client_id(self):
        """
        Fetch the client id for the transport.
        """
        raise NotImplementedError

    def create_search_index(self, index, schema=None, n_val=None,
                            timeout=None):
        """
        Creates a yokozuna search index.
        """
        raise NotImplementedError

    def get_search_index(self, index):
        """
        Returns a yokozuna search index or None.
        """
        raise NotImplementedError

    def list_search_indexes(self):
        """
        Lists all yokozuna search indexes.
        """
        raise NotImplementedError

    def delete_search_index(self, index):
        """
        Deletes a yokozuna search index.
        """
        raise NotImplementedError

    def create_search_schema(self, schema, content):
        """
        Creates a yokozuna search schema.
        """
        raise NotImplementedError

    def get_search_schema(self, schema):
        """
        Returns a yokozuna search schema.
        """
        raise NotImplementedError

    def search(self, index, query, **params):
        """
        Performs a search query.
        """
        raise NotImplementedError

    def get_index(self, bucket, index, startkey, endkey=None,
                  return_terms=None, max_results=None, continuation=None,
                  timeout=None, term_regex=None):
        """
        Performs a secondary index query.
        """
        raise NotImplementedError

    def stream_index(self, bucket, index, startkey, endkey=None,
                     return_terms=None, max_results=None, continuation=None,
                     timeout=None):
        """
        Streams a secondary index query.
        """
        raise NotImplementedError

    def fulltext_add(self, index, *docs):
        """
        Adds documents to the full-text index.
        """
        raise NotImplementedError

    def fulltext_delete(self, index, docs=None, queries=None):
        """
        Removes documents from the full-text index.
        """
        raise NotImplementedError

    def get_counter(self, bucket, key, r=None, pr=None, basic_quorum=None,
                    notfound_ok=None):
        """
        Gets the value of a counter.
        """
        raise NotImplementedError

    def update_counter(self, bucket, key, value, w=None, dw=None, pw=None,
                       returnvalue=False):
        """
        Updates a counter by the given value.
        """
        raise NotImplementedError

    def fetch_datatype(self, bucket, key, r=None, pr=None, basic_quorum=None,
                       notfound_ok=None, timeout=None, include_context=None):
        """
        Fetches a Riak Datatype.
        """
        raise NotImplementedError

    def update_datatype(self, datatype, w=None, dw=None, pw=None,
                        return_body=None, timeout=None, include_context=None):
        """
        Updates a Riak Datatype by sending local operations to the server.
        """
        raise NotImplementedError

    def get_preflist(self, bucket, key):
        """
        Fetches the preflist for a bucket/key.
        """
        raise NotImplementedError

    # TODO FUTURE NUKE THIS MAPRED
    def _search_mapred_emu(self, index, query):
        """
        Emulates a search request via MapReduce. Used in the case
        where the transport supports MapReduce but has no native
        search capability.
        """
        phases = []
        if not self.phaseless_mapred():
            phases.append({'language': 'erlang',
                           'module': 'riak_kv_mapreduce',
                           'function': 'reduce_identity',
                           'keep': True})
        mr_result = self.mapred({'module': 'riak_search',
                                 'function': 'mapred_search',
                                 'arg': [index, query]},
                                phases)
        result = {'num_found': len(mr_result),
                  'max_score': 0.0,
                  'docs': []}
        for bucket, key, data in mr_result:
            if u'score' in data and data[u'score'][0] > result['max_score']:
                result['max_score'] = data[u'score'][0]
            result['docs'].append({u'id': key})
        return result

    # TODO FUTURE NUKE THIS MAPRED
    def _get_index_mapred_emu(self, bucket, index, startkey, endkey=None):
        """
        Emulates a secondary index request via MapReduce. Used in the
        case where the transport supports MapReduce but has no native
        secondary index query capability.
        """
        phases = []
        if not self.phaseless_mapred():
            phases.append({'language': 'erlang',
                           'module': 'riak_kv_mapreduce',
                           'function': 'reduce_identity',
                           'keep': True})
        if endkey:
            result = self.mapred({'bucket': bucket,
                                  'index': index,
                                  'start': startkey,
                                  'end': endkey},
                                 phases)
        else:
            result = self.mapred({'bucket': bucket,
                                  'index': index,
                                  'key': startkey},
                                 phases)
        return [key for resultbucket, key in result]

    def _construct_mapred_json(self, inputs, query, timeout=None):
        if not self.phaseless_mapred() and (query is None or len(query) is 0):
            raise Exception(
                'Phase-less MapReduce is not supported by Riak node')

        job = {'inputs': inputs, 'query': query}
        if timeout is not None:
            job['timeout'] = timeout

        content = json.dumps(job)
        return content

    def _check_bucket_types(self, bucket_type):
        if not self.bucket_types():
            raise NotImplementedError('Server does not support bucket-types')
        if bucket_type.is_default():
            raise ValueError('Cannot manipulate the default bucket-type')
