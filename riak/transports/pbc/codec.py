"""
Copyright 2012 Basho Technologies, Inc.

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
import riak_pb
from riak.riak_object import RiakObject

RIAKC_RW_ONE = 4294967294
RIAKC_RW_QUORUM = 4294967293
RIAKC_RW_ALL = 4294967292
RIAKC_RW_DEFAULT = 4294967291


class RiakPbcCodec(object):
    """
    Protobuffs Encoding and decoding methods for RiakPbcTransport.
    """

    rw_names = {
        'default': RIAKC_RW_DEFAULT,
        'all': RIAKC_RW_ALL,
        'quorum': RIAKC_RW_QUORUM,
        'one': RIAKC_RW_ONE
    }

    def __init__(self, **unused_args):
        if riak_pb is None:
            raise NotImplementedError("this transport is not available")
        super(RiakPbcCodec, self).__init__(**unused_args)

    def translate_rw_val(self, rw):
        """
        Converts a symbolic quorum value into its on-the-wire
        equivalent.

        :param rw: the quorum
        :type rw: string, integer
        :rtype: integer
        """
        val = self.rw_names.get(rw)
        if val is None:
            return rw
        elif type(rw) is int and rw >= 0:
            return val
        else:
            return None

    def decode_content(self, rpb_content, robj):
        """
        Decodes a single sibling from the protobuf representation into
        a RiakObject.

        :rtype: (RiakObject)
        """

        if rpb_content.HasField("deleted"):
            robj.deleted = True
        if rpb_content.HasField("content_type"):
            robj.content_type = rpb_content.content_type
        if rpb_content.HasField("charset"):
            robj.charset = rpb_content.charset
        if rpb_content.HasField("content_encoding"):
            robj.content_encoding = rpb_content.content_encoding
        if rpb_content.HasField("vtag"):
            robj.vtag = rpb_content.vtag
        links = []
        for link in rpb_content.links:
            if link.HasField("bucket"):
                bucket = link.bucket
            else:
                bucket = None
            if link.HasField("key"):
                key = link.key
            else:
                key = None
            if link.HasField("tag"):
                tag = link.tag
            else:
                tag = None
            links.append((bucket, key, tag))
        if links:
            robj.links = links
        if rpb_content.HasField("last_mod"):
            robj.last_mod = rpb_content.last_mod
        if rpb_content.HasField("last_mod_usecs"):
            robj.last_mod_usecs = rpb_content.last_mod_usecs
        usermeta = {}
        for usermd in rpb_content.usermeta:
            usermeta[usermd.key] = usermd.value
        if len(usermeta) > 0:
            robj.usermeta = usermeta
        indexes = set()
        for index in rpb_content.indexes:
            if index.key.endswith("_int"):
                indexes.add((index.key, int(index.value)))
            else:
                indexes.add((index.key, index.value))

        if len(indexes) > 0:
            robj.indexes = indexes

        robj.set_encoded_data(rpb_content.value)
        robj.exists = True

        return robj

    def encode_content(self, robj, rpb_content):
        """
        Fills an RpbContent message with the appropriate data and
        metadata from a RiakObject.
        """
        if robj.content_type:
            rpb_content.content_type = robj.content_type
        if robj.charset:
            rpb_content.charset = robj.charset
        if robj.content_encoding:
            rpb_content.content_encoding = robj.content_encoding
        for uk in robj.usermeta:
            pair = rpb_content.usermeta.add()
            pair.key = uk
            pair.value = robj.usermeta[uk]
        for link in robj.links:
            pb_link = rpb_content.links.add()
            try:
                bucket, key, tag = link
            except ValueError:
                raise RiakError("Invalid link tuple %s" % link)

            pb_link.bucket = bucket
            pb_link.key = key
            if tag:
                pb_link.tag = tag
            else:
                pb_link.tag = ''

        for field, value in robj.indexes:
                    pair = rpb_content.indexes.add()
                    pair.key = field
                    pair.value = str(value)

        rpb_content.value = str(robj.get_encoded_data())
