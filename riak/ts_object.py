# Copyright 2010-present Basho Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import collections

from riak import RiakError
from riak.table import Table

TsColumns = collections.namedtuple('TsColumns', ['names', 'types'])


class TsObject(object):
    """
    The TsObject holds information about Timeseries data, plus the data
    itself.
    """
    def __init__(self, client, table, rows=None, columns=None):
        """
        Construct a new TsObject.

        :param client: A RiakClient object.
        :type client: :class:`RiakClient <riak.client.RiakClient>`
        :param table: The table for the timeseries data as a Table object.
        :type table: :class:`Table` <riak.table.Table>
        :param rows: An list of lists with timeseries data
        :type rows: list
        :param columns: A TsColumns tuple. Optional
        :type columns: :class:`TsColumns` <riak.TsColumns>
        """

        if not isinstance(table, Table):
            raise ValueError('table must be an instance of Table.')

        self.client = client
        self.table = table

        if rows is not None and not isinstance(rows, list):
            raise RiakError("TsObject rows parameter must be a list.")
        else:
            self.rows = rows

        if columns is not None and \
           not isinstance(columns, TsColumns):
            raise RiakError(
                "TsObject columns parameter must be a TsColumns instance")
        else:
            self.columns = columns

    def store(self):
        """
        Store the timeseries data in Riak.
        :rtype: boolean
        """
        return self.client.ts_put(self)
