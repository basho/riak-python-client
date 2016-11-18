"""
Copyright 2013 Basho Technologies, Inc.

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
from six import string_types


class RiakContent(object):
    """
    The RiakContent holds the metadata and value of a single sibling
    within a RiakObject. RiakObjects that have more than one sibling
    are considered to be in conflict.
    """
    def __init__(self, robject, data=None, encoded_data=None, charset=None,
                 content_type='application/json', content_encoding=None,
                 last_modified=None, etag=None, usermeta=None, links=None,
                 indexes=None, exists=False):
        self._robject = robject
        self._data = data
        self._encoded_data = encoded_data
        self.charset = charset
        self.content_type = content_type
        self.content_encoding = content_encoding
        self.last_modified = last_modified
        self.etag = etag
        self.usermeta = usermeta or {}
        self.links = links or []
        self.indexes = indexes or set()
        self.exists = exists

    def _get_data(self):
        if self._encoded_data is not None and self._data is None:
            self._data = self._deserialize(self._encoded_data)
            self._encoded_data = None
        return self._data

    def _set_data(self, value):
        self._encoded_data = None
        self._data = value

    data = property(_get_data, _set_data, doc="""
        The data stored in this object, as Python objects. For the raw
        data, use the `encoded_data` property. If unset, accessing
        this property will result in decoding the `encoded_data`
        property into Python values. The decoding is dependent on the
        `content_type` property and the bucket's registered decoders.
        :type mixed """)

    def _get_encoded_data(self):
        if self._data is not None and self._encoded_data is None:
            self._encoded_data = self._serialize(self._data)
            self._data = None
        return self._encoded_data

    def _set_encoded_data(self, value):
        self._data = None
        self._encoded_data = value

    encoded_data = property(_get_encoded_data, _set_encoded_data, doc="""
        The raw data stored in this object, essentially the encoded
        form of the `data` property. If unset, accessing this property
        will result in encoding the `data` property into a string. The
        encoding is dependent on the `content_type` property and the
        bucket's registered encoders.
        :type str""")

    def _serialize(self, value):
        encoder = self._robject.bucket.get_encoder(self.content_type)
        if encoder:
            return encoder(value)
        elif isinstance(value, string_types):
            return value.encode()
        else:
            raise TypeError('No encoder for non-string data '
                            'with content type "{0}"'.
                            format(self.content_type))

    def _deserialize(self, value):
        if not value:
            return value
        decoder = self._robject.bucket.get_decoder(self.content_type)
        if decoder:
            return decoder(value)
        else:
            raise TypeError('No decoder for content type "{0}"'.
                            format(self.content_type))

    def add_index(self, field, value):
        """
        add_index(field, value)

        Tag this object with the specified field/value pair for
        indexing.

        :param field: The index field.
        :type field: string
        :param value: The index value.
        :type value: string or integer
        :rtype: :class:`RiakObject <riak.riak_object.RiakObject>`
        """
        if field[-4:] not in ("_bin", "_int"):
            raise RiakError("Riak 2i fields must end with either '_bin'"
                            " or '_int'.")

        self.indexes.add((field, value))

        return self._robject

    def remove_index(self, field=None, value=None):
        """
        remove_index(field=None, value=None)

        Remove the specified field/value pair as an index on this
        object.

        :param field: The index field.
        :type field: string
        :param value: The index value.
        :type value: string or integer
        :rtype: :class:`RiakObject <riak.riak_object.RiakObject>`
        """
        if not field and not value:
            self.indexes.clear()
        elif field and not value:
            for index in [x for x in self.indexes if x[0] == field]:
                self.indexes.remove(index)
        elif field and value:
            self.indexes.remove((field, value))
        else:
            raise RiakError("Cannot pass value without a field"
                            " name while removing index")

        return self._robject

    remove_indexes = remove_index

    def set_index(self, field, value):
        """
        set_index(field, value)

        Works like :meth:`add_index`, but ensures that there is only
        one index on given field. If other found, then removes it
        first.

        :param field: The index field.
        :type field: string
        :param value: The index value.
        :type value: string or integer
        :rtype: :class:`RiakObject <riak.riak_object.RiakObject>`
        """
        to_rem = set((x for x in self.indexes if x[0] == field))
        self.indexes.difference_update(to_rem)
        return self.add_index(field, value)

    def add_link(self, obj, tag=None):
        """
        add_link(obj, tag=None)

        Add a link to a RiakObject.

        :param obj: Either a RiakObject or 3 item link tuple consisting
            of (bucket, key, tag).
        :type obj: mixed
        :param tag: Optional link tag. Defaults to bucket name. It is ignored
            if ``obj`` is a 3 item link tuple.
        :type tag: string
        :rtype: :class:`RiakObject <riak.riak_object.RiakObject>`
        """
        if isinstance(obj, tuple):
            newlink = obj
        else:
            newlink = (obj.bucket.name, obj.key, tag)

        self.links.append(newlink)
        return self._robject
