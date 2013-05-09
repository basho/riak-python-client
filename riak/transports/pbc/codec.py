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
from riak import RiakError
from riak.content import RiakContent

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

    def _decode_contents(self, contents, obj):
        obj.siblings = [self._decode_content(c, RiakContent(obj))
                        for c in contents]
        return obj

    def _decode_content(self, rpb_content, sibling):
        """
        Decodes a single sibling from the protobuf representation into
        a RiakObject.

        :rtype: (RiakObject)
        """

        if rpb_content.HasField("deleted") and rpb_content.deleted:
            sibling.exists = False
        else:
            sibling.exists = True
        if rpb_content.HasField("content_type"):
            sibling.content_type = rpb_content.content_type
        if rpb_content.HasField("charset"):
            sibling.charset = rpb_content.charset
        if rpb_content.HasField("content_encoding"):
            sibling.content_encoding = rpb_content.content_encoding
        if rpb_content.HasField("vtag"):
            sibling.etag = rpb_content.vtag

        sibling.links = [self._decode_link(link)
                         for link in rpb_content.links]
        if rpb_content.HasField("last_mod"):
            sibling.last_modified = float(rpb_content.last_mod)
            if rpb_content.HasField("last_mod_usecs"):
                sibling.last_modified += rpb_content.last_mod_usecs / 1000000.0

        sibling.usermeta = dict([(usermd.key, usermd.value)
                                 for usermd in rpb_content.usermeta])
        sibling.indexes = set([(index.key,
                                self._decode_index_value(index.key,
                                                         index.value))
                               for index in rpb_content.indexes])

        sibling.encoded_data = rpb_content.value

        return sibling

    def _encode_content(self, robj, rpb_content):
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

        rpb_content.value = str(robj.encoded_data)

    def _decode_link(self, link):
        """
        Decodes an RpbLink message into a tuple
        """

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

        return (bucket, key, tag)

    def _decode_index_value(self, index, value):
        """
        Decodes a secondary index value into the correct Python type.
        """
        if index.endswith("_int"):
            return int(value)
        else:
            return value
