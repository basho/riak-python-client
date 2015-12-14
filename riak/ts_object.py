from riak import RiakError
from riak.table import Table


class TsObject(object):
    """
    The TsObject holds information about Timeseries data, plus the data
    itself.
    """
    def __init__(self, client, table, rows=[], columns=[]):
        """
        Construct a new TsObject.

        :param client: A RiakClient object.
        :type client: :class:`RiakClient <riak.client.RiakClient>`
        :param table: The table for the timeseries data as a Table object.
        :type table: :class:`Table` <riak.table.Table>
        :param rows: An list of lists with timeseries data
        :type rows: list
        :param columns: An list of Column names and types. Optional.
        :type columns: list
        """

        if not isinstance(table, Table):
            raise ValueError('table must be an instance of Table.')

        self.client = client
        self.table = table

        self.rows = rows
        if not isinstance(self.rows, list):
            raise RiakError("TsObject requires a list of rows")

        self.columns = columns
        if self.columns is not None and not isinstance(self.columns, list):
            raise RiakError("TsObject columns must be a list")

    def store(self):
        """
        Store the timeseries data in Riak.
        :rtype: boolean
        """
        return self.client.ts_put(self)
