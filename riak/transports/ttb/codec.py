import datetime
import logging

from erlastic import decode, encode
from erlastic.types import Atom
from six import string_types, PY2

from riak import RiakError
from riak.util import str_to_bytes, bytes_to_str, \
    unix_time_millis, datetime_from_unix_time_millis

udef_a = Atom('undefined')

rpberrorresp_a = Atom('rpberrorresp')
tsgetreq_a = Atom('tsgetreq')
tsgetresp_a = Atom('tsgetresp')
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

    def _encode_timeseries_keyreq_ttb(self, table, key):
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

    def _decode_timeseries_ttb(self, resp_ttb, tsobj):
        """
        Fills an TsObject with the appropriate data and
        metadata from a TTB-encoded TsGetResp / TsQueryResp.

        :param resp_ttb: the protobuf message from which to process data
        :type resp_ttb: TTB-encoded tsqueryrsp or tsgetresp
        :param tsobj: a TsObject
        :type tsobj: TsObject
        """
        # if tsobj.columns is not None:
        #     for col in resp.columns:
        #         col_name = bytes_to_str(col.name)
        #         col_type = col.type
        #         col = (col_name, col_type)
        #         tsobj.columns.append(col)
        resp = decode(resp_ttb)
        resp_a = resp[0]
        if resp_a == tsgetresp_a:
            resp_cols = resp[1]
            resp_rows = resp[2]
            for row_ttb in resp_rows:
                tsobj.rows.append(
                    self._decode_timeseries_row(row_ttb, None)) # TODO cols
        # elif resp_a == rpberrorresp_a:
        else:
            raise RiakError("Unknown TTB response type: {}".format(resp_a))

    def _decode_timeseries_row(self, tsrow_ttb, tscols=None):
        """
        Decodes a TTB-encoded TsRow into a list

        :param tsrow: the TTB-encoded TsRow to decode.
        :type tsrow: TTB encoded row
        :param tscols: the TTB-encoded TsColumn data to help decode.
        :type tscols: list
        :rtype list
        """
        row = []
        for tsc_ttb in tsrow_ttb:
            if tsc_ttb == tscell_empty:
                row.append(None)
            else:
                val = None
                if tsc_ttb[0] == tscell_a:
                    if tsc_ttb[1] != udef_a:
                        row.append(bytes_to_str(tsc_ttb[1]))
                    elif tsc_ttb[2] != udef_a:
                        row.append(tsc_ttb[2])
                    elif tsc_ttb[3] != udef_a:
                        row.append(
                            datetime_from_unix_time_millis(tsc_ttb[3]))
                    elif tsc_ttb[4] != udef_a:
                        row.append(tsc_ttb[4])
                    elif tsc_ttb[5] != udef_a:
                        row.append(tsc_ttb[5])
                    else:
                        row.append(None)
                else:
                    raise RiakError(
                        "Expected tscell atom, got: {}".format(tsc_ttb))
        return row
