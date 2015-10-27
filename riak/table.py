from six import string_types, PY2

class Table(object):
    """
    The ``Table`` object allows you to access properties on a Riak
    timeseries table and query timeseries data.
    """
    def __init__(self, client, name):
        """
        Returns a new ``Table`` instance.

        :param client: A :class:`RiakClient <riak.client.RiakClient>`
               instance
        :type client: :class:`RiakClient <riak.client.RiakClient>`
        :param name: The table's name
        :type name: string
        """

        if not isinstance(name, string_types):
            raise TypeError('Table name must be a string')

        if PY2:
            try:
                name = name.encode('ascii')
            except UnicodeError:
                raise TypeError('Unicode table names are not supported.')

        self._client = client
        self.name = name

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    def new(self, rows, columns=None):
        """
        A shortcut for manually instantiating a new :class:`~riak.ts_object.TsObject`

        :param rows: An list of lists with timeseries data
        :type rows: list
        :param columns: An list of Column names and types. Optional.
        :type columns: list
        :rtype: :class:`~riak.ts_object.TsObject`
        """
        from riak.ts_object import TsObject

        return TsObject(self._client, self, rows, columns)

    def get(self, table, key):
        """
        Gets a value from a timeseries table.

        :param table: The timeseries table.
        :type table: string or :class:`Table <riak.table.Table>`
        :param key: The timeseries value's key.
        :type key: list or dict
        :rtype: :class:`TsObject <riak.ts_object.TsObject>`
        """
        return self.client.ts_get(self, table, key)

    def delete(self, table, key):
        """
        Deletes a value from a timeseries table.

        :param table: The timeseries table.
        :type table: string or :class:`Table <riak.table.Table>`
        :param key: The timeseries value's key.
        :type key: list or dict
        :rtype: boolean
        """
        return self.client.ts_delete(self, table, key)

    def query(self, query, interpolations=None):
        """
        Queries a timeseries table.

        :param query: The timeseries query.
        :type query: string
        :rtype: :class:`TsObject <riak.ts_object.TsObject>`
        """
        return self.client.ts_query(self, query, interpolations)
