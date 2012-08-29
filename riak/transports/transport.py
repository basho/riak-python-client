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
from riak import RiakError
import base64
import random
import threading
import platform
import os
from feature_detect import FeatureDetection


class RiakTransport(FeatureDetection):
    """
    Class to encapsulate transport details
    """

    # Subclasses should specify their API level.
    #   * missing or 1: the API used up and through 1.3.x.
    #   * 2: the API introduced with 1.4.x
    #
    # api = 2

    @classmethod
    def make_random_client_id(self):
        """
        Returns a random client identifier
        """
        return 'py_%s' % base64.b64encode(
                str(random.randint(1, 0x40000000)))

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
        @return boolean
        """
        raise RiakError("not implemented")

    def get(self, robj, r=None, vtag=None):
        """
        Serialize get request and deserialize response
        @return (vclock=None, [(metadata, value)]=None)
        """
        raise RiakError("not implemented")

    def put(self, robj, w=None, dw=None, return_body=True):
        """
        Serialize put request and deserialize response - if 'content'
        is true, retrieve the updated metadata/content
        @return (vclock=None, [(metadata, value)]=None)
        """
        raise RiakError("not implemented")

    def put_new(self, robj, w=None, dw=None, return_meta=True):
        """Put a new object into the Riak store, returning its (new) key.

        If return_meta is False, then the vlock and metadata return values
        will be None.

        @return (key, vclock, metadata)
        """
        raise RiakError("not implemented")

    def delete(self, robj, rw=None):
        """
        Serialize delete request and deserialize response
        @return true
        """
        raise RiakError("not implemented")

    def get_buckets(self):
        """
        Serialize get buckets request and deserialize response
        @return dict()
        """
        raise RiakError("not implemented")

    def get_bucket_props(self, bucket):
        """
        Serialize get bucket property request and deserialize response
        @return dict()
        """
        raise RiakError("not implemented")

    def set_bucket_props(self, bucket, props):
        """
        Serialize set bucket property request and deserialize response
        bucket = bucket object
        props = dictionary of properties
        @return boolean
        """
        raise RiakError("not implemented")

    def mapred(self, inputs, query, timeout=None):
        """
        Serialize map/reduce request
        """
        raise RiakError("not implemented")

    def set_client_id(self, client_id):
        """
        Set the client id. This overrides the default, random client
        id, which is automatically generated when none is specified in
        when creating the transport object.
        """
        raise RiakError("not implemented")

    def get_client_id(self):
        """
        Fetch the client id for the transport.
        """
        raise RiakError("not implemented")

    def search(self, index, query, **params):
        """
        Performs a search query.
        """
        raise RiakError("not implemented")

    def get_index(self, bucket, index, startkey, endkey=None):
        """
        Performs a secondary index query.
        """
        raise RiakError("not implemented")

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
        return [key for bucket, key in result]

    def store_file(self, key, content_type="application/octet-stream",
                   content=None):
        """
        Store a large piece of data in luwak.
        key = the key/filename for the object
        content_type = the object's content type
        content = the object's data
        """
        raise RiakError("luwak not supported by this transport.")

    def get_file(self, key):
        """
        Get an object from luwak.
        key = the object's key
        """
        raise RiakError("luwak not supported by this transport.")

    def delete_file(self, key):
        """
        Delete an object in luwak.
        key = the object's key
        """
        raise RiakError("luwak not supported by this transport.")
