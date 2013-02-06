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
from riak.metadata import (
        MD_CHARSET,
        MD_CTYPE,
        MD_ENCODING,
        MD_INDEX,
        MD_LASTMOD,
        MD_LASTMOD_USECS,
        MD_LINKS,
        MD_USERMETA,
        MD_VTAG,
        MD_DELETED
        )

import riak_pb
from riak.mapreduce import RiakLink

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

    def decode_contents(self, rpb_contents):
        """
        Decodes the multiple contents (siblings) of a RiakObject from
        its protobuf representation.
        """
        return [self.decode_content(rpb_c) for rpb_c in rpb_contents]

    def decode_content(self, rpb_content):
        """
        Decodes a single sibling from the protobuf representation into
        its metadata and value.

        :rtype: (dict, string)
        """
        metadata = {}
        if rpb_content.HasField("deleted"):
            metadata[MD_DELETED] = True
        if rpb_content.HasField("content_type"):
            metadata[MD_CTYPE] = rpb_content.content_type
        if rpb_content.HasField("charset"):
            metadata[MD_CHARSET] = rpb_content.charset
        if rpb_content.HasField("content_encoding"):
            metadata[MD_ENCODING] = rpb_content.content_encoding
        if rpb_content.HasField("vtag"):
            metadata[MD_VTAG] = rpb_content.vtag
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
            links.append(RiakLink(bucket, key, tag))
        if links:
            metadata[MD_LINKS] = links
        if rpb_content.HasField("last_mod"):
            metadata[MD_LASTMOD] = rpb_content.last_mod
        if rpb_content.HasField("last_mod_usecs"):
            metadata[MD_LASTMOD_USECS] = rpb_content.last_mod_usecs
        usermeta = {}
        for usermd in rpb_content.usermeta:
            usermeta[usermd.key] = usermd.value
        if len(usermeta) > 0:
            metadata[MD_USERMETA] = usermeta
        indexes = []
        for index in rpb_content.indexes:
            if index.key.endswith("_int"):
                value = int(index.value)
            else:
                value = index.value
            rie = (index.key, value)
            indexes.append(rie)
        if len(indexes) > 0:
            metadata[MD_INDEX] = indexes
        return metadata, rpb_content.value

    def encode_content(self, metadata, data, rpb_content):
        """
        Fills an RpbContent message with the appropriate data and
        metadata from a RiakObject.
        """
        # Convert the broken out fields, building up
        # pbmetadata for any unknown ones
        for k in metadata:
            v = metadata[k]
            if k == MD_CTYPE:
                rpb_content.content_type = v
            elif k == MD_CHARSET:
                rpb_content.charset = v
            elif k == MD_ENCODING:
                rpb_content.content_encoding = v
            elif k == MD_USERMETA:
                for uk in v:
                    pair = rpb_content.usermeta.add()
                    pair.key = uk
                    pair.value = v[uk]
            elif k == MD_INDEX:
                for field, value in v:
                    pair = rpb_content.indexes.add()
                    pair.key = field
                    pair.value = str(value)
            elif k == MD_LINKS:
                for link in v:
                    pb_link = rpb_content.links.add()
                    pb_link.bucket = link.bucket
                    pb_link.key = link.key
                    pb_link.tag = link.tag
        rpb_content.value = str(data)
