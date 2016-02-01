import erlastic
import datetime
import logging

from riak import RiakError
from riak.content import RiakContent
from riak.util import decode_index_value, str_to_bytes, bytes_to_str, \
    unix_time_millis, datetime_from_unix_time_millis
from six import string_types, PY2


class RiakTtbCodec(object):
    '''
    Erlang term-to-binary Encoding and decoding methods for RiakTtbTransport
    '''

    def __init__(self, **unused_args):
        super(RiakTtbCodec, self).__init__(**unused_args)

    def _encode_to_ts_cell(self, cell, ts_cell):
        if cell is not None:
            if isinstance(cell, datetime.datetime):
                ts_cell.timestamp_value = unix_time_millis(cell)
            elif isinstance(cell, bool):
                ts_cell.boolean_value = cell
            elif isinstance(cell, string_types):
                logging.debug("cell -> str: '%s'", cell)
                ts_cell.varchar_value = str_to_bytes(cell)
            elif (isinstance(cell, int) or
                 (PY2 and isinstance(cell, long))):  # noqa
                logging.debug("cell -> int/long: '%s'", cell)
                ts_cell.sint64_value = cell
            elif isinstance(cell, float):
                ts_cell.double_value = cell
            else:
                t = type(cell)
                raise RiakError("can't serialize type '{}', value '{}'"
                                .format(t, cell))

    def _encode_timeseries_keyreq(self, table, key):
        key_vals = None
        if isinstance(key, list):
            key_vals = key
        else:
            raise ValueError("key must be a list")
        return None
