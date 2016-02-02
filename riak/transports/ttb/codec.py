import datetime
import logging

from erlastic import decode, encode
from erlastic.types import Atom

from riak.util import str_to_bytes, bytes_to_str, \
    unix_time_millis, datetime_from_unix_time_millis
from six import string_types, PY2

udef_a = Atom('undefined')

tsgetreq_a = Atom('tsgetreq')
tsputreq_a = Atom('tsputreq')
tscell_a = Atom('tscell')

tscell_empty = (tscell_a, udef_a, udef_a, udef_a, udef_a, udef_a)

class RiakTtbCodec(object):
    '''
    Erlang term-to-binary Encoding and decoding methods for RiakTtbTransport
    '''

    def __init__(self, **unused_args):
        super(RiakTtbCodec, self).__init__(**unused_args)

    def _encode_to_ts_cell(self, cell):
        if cell is None:
            return tscell_empty
        else:
            if isinstance(cell, datetime.datetime):
                ts = unix_time_millis(cell)
                logging.debug("cell -> timestamp: '%s'", ts)
                return (tscell_a, udef_a, udef_a, ts, udef_a, udef_a)
            elif isinstance(cell, bool):
                logging.debug("cell -> bool: '%s'", cell)
                return (tscell_a, udef_a, udef_a, udef_a, cell, udef_a)
            elif isinstance(cell, string_types):
                logging.debug("cell -> str: '%s'", cell)
                return (tscell_a, str_to_bytes(cell),
                        udef_a, udef_a, udef_a, udef_a)
            elif (isinstance(cell, int) or
                 (PY2 and isinstance(cell, long))):  # noqa
                logging.debug("cell -> int/long: '%s'", cell)
                return (tscell_a, udef_a, cell, udef_a, udef_a, udef_a)
            elif isinstance(cell, float):
                logging.debug("cell -> float: '%s'", cell)
                return (tscell_a, udef_a, udef_a, udef_a, udef_a, cell)
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
        req = tsgetreq_a, str_to_bytes(table.name), \
            [self._encode_to_ts_cell(k) for k in key_vals], udef_a
        return encode(req)

    def _encode_timeseries_put(self, tsobj):
        '''
        Returns an Erlang-TTB encoded tuple with the appropriate data and
        metadata from a TsObject.

        :param tsobj: a TsObject
        :type tsobj: TsObject
        :rtype: term-to-binary encoded object
        '''
        if tsobj.columns:
            raise NotImplementedError("columns are not implemented yet")

        if tsobj.rows and isinstance(tsobj.rows, list):
            req_rows = []
            for row in tsobj.rows:
                req_r = []
                for cell in row:
                    req_r.append(self._encode_to_ts_cell(cell))
                req_rows.append(req_r)
            req = tsputreq_a, str_to_bytes(tsobj.table.name), \
                  udef_a, req_rows
            return encode(req)
        else:
            raise RiakError("TsObject requires a list of rows")
