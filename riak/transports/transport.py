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
import base64
import random
import threading
import platform
import os
import json
from feature_detect import FeatureDetection


class RiakTransport(FeatureDetection):
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
        return ('py_%s' %
                base64.b64encode(str(random.randint(1, 0x40000000))))

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

    def get(self, robj, r=None, pr=None, timeout=None):
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

    def get_buckets(self, timeout=None):
        """
        Gets the list of buckets as strings.
        """
        raise NotImplementedError

    def stream_buckets(self, timeout=None):
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

    def search(self, index, query, **params):
        """
        Performs a search query.
        """
        raise NotImplementedError

    def get_index(self, bucket, index, startkey, endkey=None,
                  return_terms=None, max_results=None, continuation=None,
                  timeout=None):
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
