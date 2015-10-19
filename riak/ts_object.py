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

# TODO RTS-367
# Should the table parameter be its own object that has a query method on it?
# Like Bucket?
class TsObject(object):
    """
    The TsObject holds meta information about Timeseries data,
    plus the data itself.
    """
    def __init__(self, client, table, rows, columns=None):
        """
        Construct a new TsObject.

        :param client: A RiakClient object.
        :type client: :class:`RiakClient <riak.client.RiakClient>`
        :param table: The table for the timeseries data as a Table object.
        :type table: :class:`Table` <riak.table.Table>
        :param rows: An array of arrays with timeseries data
        :type rows: array
        :param columns: An array Column names and types. Optional.
        :type columns: array
        """

        if table is None or len(table) == 0:
            raise ValueError('Table must either be a non-empty string.')

        self.client = client
        self.table = table
        # TODO RTS-367 rows, columns

    def store(self):
        """
        Store the timeseries data in Riak.
        :rtype: boolean
        """

        return self.client.ts_put(self)
