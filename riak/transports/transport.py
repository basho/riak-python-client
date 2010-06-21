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

class RiakTransport(object):
    """
    Class to encapsulate transport details
    """

    @classmethod
    def make_random_client_id(self):
        '''
        Returns a random client identifier
        '''
        return 'py_%s' % base64.b64encode(
                str(random.randint(1, 1073741824)))

    @classmethod
    def make_fixed_client_id(self):
        '''
        Returns a unique identifier for the current machine/process/thread.
        '''
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

    def get(self, robj, r = None, vtag = None):
        """
        Serialize get request and deserialize response
        @return (vclock=None, [(metadata, value)]=None)
        """
        raise RiakError("not implemented")

    def put(self, robj, w = None, dw = None, return_body = True):
        """
        Serialize put request and deserialize response - if 'content'
        is true, retrieve the updated metadata/content
        @return (vclock=None, [(metadata, value)]=None)
        """
        raise RiakError("not implemented")

    def delete(self, robj, rw = None):
        """
        Serialize delete request and deserialize response
        @return true
        """
        raise RiakError("not implemented")

    def get_bucket_props(self, bucket) :
        """
        Serialize get bucket property request and deserialize response
        @return dict()
        """
        raise RiakError("not implemented")

    def set_bucket_props(self, bucket, props) :
        """
        Serialize set bucket property request and deserialize response
        bucket = bucket object
        props = dictionary of properties
        @return boolean
        """
        raise RiakError("not implemented")

    def mapred(self, inputs, query, timeout = None) :
        """
        Serialize map/reduce request
        """
        raise RiakError("not implemented")

    def set_client_id(self, client_id):
        raise RiakError("not implemented")

