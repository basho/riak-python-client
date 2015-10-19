"""
Copyright 2015 Basho Technologies <dev@basho.com>

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
from six import string_types, PY2


class RiakTable(object):
    """
    The ``RiakTable`` object allows you to access properties on a Riak table
    (bucket type) and query timeseries data.
    """
    def __init__(self, client, name):
        """
        Returns a new ``Table`` instance.

        :param client: A :class:`RiakClient <riak.client.RiakClient>`
               instance
        :type client: :class:`RiakClient <riak.client.RiakClient>`
        :param name: The tables's name
        :type name: string
        """

        if not isinstance(name, string_types):
            raise TypeError('Bucket name must be a string')

        if PY2:
            try:
                name = name.encode('ascii')
            except UnicodeError:
                raise TypeError('Unicode table names are not supported.')

        self._client = client
        self.name = name

    def query(self, key):
        """
        Retrieve a bucket-type property.

        :param key: The property to retrieve.
        :type key: string
        :rtype: mixed
        """
        return self.get_properties()[key]
