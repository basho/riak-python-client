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
from riak_object import RiakObject

class RiakBucket(object):
    """
    The RiakBucket object allows you to access and change information
    about a Riak bucket, and provides methods to create or retrieve
    objects within the bucket.
    """

    def __init__(self, client, name):
        self._client = client
        self._name = name
        self._r = None
        self._w = None
        self._dw = None
        self._rw = None
        self._encoders = {}
        self._decoders = {}

    def get_name(self):
        """
        Get the bucket name.
        """
        return self._name

    def get_r(self, r=None):
        """
        Get the R-value for this bucket, if it is set, otherwise return
        the R-value for the client.
        @return integer
        """
        if (r is not None):
            return r
        if (self._r is not None):
            return self._r
        return self._client.get_r()

    def set_r(self, r):
        """
        Set the R-value for this bucket. get(...) and get_binary(...)
        operations that do not specify an R-value will use this value.
        @param integer r - The new R-value.
        @return self
        """
        self._r = r
        return self

    def get_w(self, w):
        """
        Get the W-value for this bucket, if it is set, otherwise return
        the W-value for the client.
        @return integer
        """
        if (w is not None):
            return w
        if (self._w is not None):
            return self._w
        return self._client.get_w()

    def set_w(self, w):
        """
        Set the W-value for this bucket. See set_r(...) for more information.
        @param integer w - The new W-value.
        @return self
        """
        self._w = w
        return self

    def get_dw(self, dw):
        """
        Get the DW-value for this bucket, if it is set, otherwise return
        the DW-value for the client.
        @return integer
        """
        if (dw is not None):
            return dw
        if (self._dw is not None):
            return self._dw
        return self._client.get_dw()

    def set_dw(self, dw):
        """
        Set the DW-value for this bucket. See set_r(...) for more information.
        @param integer dw - The new DW-value
        @return self
        """
        self._dw = dw
        return self

    def get_rw(self, rw):
        """
        Get the RW-value for this bucket, if it is set, otherwise return
        the RW-value for the client.
        @return integer
        """
        if (rw is not None):
            return rw
        if (self._rw is not None):
            return self._rw
        return self._client.get_rw()

    def set_rw(self, rw):
        """
        Set the RW-value for this bucket. See set_r(...) for more information.
        @param integer rw - The new RW-value
        @return self
        """
        self._rw = rw
        return self

    def get_encoder(self, content_type):
        """
        Get the encoding function for this content type for this bucket
        @param content_type: Content type requested
        """
        if content_type in self._encoders:
            return self._encoders[content_type]
        else:
            return self._client.get_encoder(content_type)

    def set_encoder(self, content_type, encoder):
        """
        Set the encoding function for this content type for this bucket
        @param content_type: Content type for encoder
        @param encoder: Function to encode with - will be called with data as single
                        argument.
        """
        self._encoders[content_type] = encoder
        return self

    def get_decoder(self, content_type):
        """
        Get the decoding function for this content type for this bucket
        @param content_type: Content type for decoder
        """
        if content_type in self._decoders:
            return self._decoders[content_type]
        else:
            return self._client.get_decoder(content_type)

    def set_decoder(self, content_type, decoder):
        """
        Set the decoding function for this content type for this bucket
        @param content_type: Content type for decoder
        @param decoder: Function to decode with - will be called with string
        """
        self._decoders[content_type] = decoder
        return self

    def new(self, key, data=None, content_type='application/json'):
        """
        Create a new Riak object that will be stored as JSON.
        @param string key - Name of the key.
        @param object data - The data to store. (default None)
        @return RiakObject
        """
        obj = RiakObject(self._client, self, key)
        obj.set_data(data)
        obj.set_content_type(content_type)
        obj._encode_data = True
        return obj

    def new_binary(self, key, data, content_type='application/octet-stream'):
        """
        Create a new Riak object that will be stored as plain text/binary.
        @param string key - Name of the key.
        @param object data - The data to store.
        @param string content_type - The content type of the object.
               (default 'application/octet-stream')
        @return RiakObject
        """
        obj = RiakObject(self._client, self, key)
        obj.set_data(data)
        obj.set_content_type(content_type)
        obj._encode_data = False
        return obj

    def get(self, key, r=None):
        """
        Retrieve a JSON-encoded object from Riak.
        @param string key - Name of the key.
        @param int r - R-Value of the request (defaults to bucket's R)
        @return RiakObject
        """
        obj = RiakObject(self._client, self, key)
        obj._encode_data = True
        r = self.get_r(r)
        return obj.reload(r)

    def get_binary(self, key, r=None):
        """
        Retrieve a binary/string object from Riak.
        @param string key - Name of the key.
        @param int r - R-Value of the request (defaults to bucket's R)
        @return RiakObject
        """
        obj = RiakObject(self._client, self, key)
        obj._encode_data = False
        r = self.get_r(r)
        return obj.reload(r)

    def set_n_val(self, nval):
        """
        Set the N-value for this bucket, which is the number of replicas
        that will be written of each object in the bucket. Set this once
        before you write any data to the bucket, and never change it
        again, otherwise unpredictable things could happen. This should
        only be used if you know what you are doing.
        @param integer nval - The new N-Val.
        """
        return self.set_property('n_val', nval)

    def get_n_val(self):
        """
        Retrieve the N-value for this bucket.
        @return integer
        """
        return self.get_property('n_val')

    def set_allow_multiples(self, bool):
        """
        If set to True, then writes with conflicting data will be stored
        and returned to the client. This situation can be detected by
        calling has_siblings() and get_siblings(). This should only be used
        if you know what you are doing.
        @param boolean bool - True to store and return conflicting writes.
        """
        return self.set_property('allow_mult', bool)

    def get_allow_multiples(self):
        """
        Retrieve the 'allow multiples' setting.
        @return Boolean
        """
        return self.get_bool_property('allow_mult')

    def set_property(self, key, value):
        """
        Set a bucket property. This should only be used if you know what
        you are doing.
        @param string key - Property to set.
        @param mixed value - Property value.
        """
        return self.set_properties({key : value})

    def get_bool_property(self, key):
        """
        Get a boolean bucket property.  Converts to a True/False value
        @param string key - Property to set.
        """
        prop = self.get_property(key)
        if prop == True or prop > 0:
            return True
        else:
            return False

    def get_property(self, key):
        """
        Retrieve a bucket property.
        @param string key - The property to retrieve.
        @return mixed
        """
        props = self.get_properties()
        if (key in props.keys()):
            return props[key]

    def set_properties(self, props):
        """
        Set multiple bucket properties in one call. This should only be
        used if you know what you are doing.
        @param array props - An associative array of key:value.
        """
        t = self._client.get_transport()
        t.set_bucket_props(self, props)

    def get_properties(self):
        """
        Retrieve an associative array of all bucket properties.
        @return Array
        """
        t = self._client.get_transport()
        return t.get_bucket_props(self)
