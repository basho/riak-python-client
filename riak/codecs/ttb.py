import datetime
import six

import riak.pb.messages

from erlastic import encode, decode
from erlastic.types import Atom

from riak import RiakError
from riak.codecs import Codec, Msg
from riak.util import bytes_to_str, unix_time_millis, \
    datetime_from_unix_time_millis

udef_a = Atom('undefined')

rpberrorresp_a = Atom('rpberrorresp')
tsgetreq_a = Atom('tsgetreq')
tsgetresp_a = Atom('tsgetresp')
tsputreq_a = Atom('tsputreq')
tsdelreq_a = Atom('tsdelreq')
tsrow_a = Atom('tsrow')
tscell_a = Atom('tscell')

tscell_empty = (tscell_a, udef_a, udef_a, udef_a, udef_a, udef_a)

# TODO RTS-842
MSG_CODE_TS_TTB = 104


class TtbCodec(Codec):
    '''
    Erlang term-to-binary Encoding and decoding methods for TcpTransport
    '''

    def __init__(self, **unused_args):
        super(TtbCodec, self).__init__(**unused_args)

    def parse_msg(self, msg_code, data):
        if msg_code != MSG_CODE_TS_TTB and \
           msg_code != riak.pb.messages.MSG_CODE_TS_GET_RESP and \
           msg_code != riak.pb.messages.MSG_CODE_TS_PUT_RESP:
            raise RiakError("TTB can't parse code: {}".format(msg_code))
        if len(data) > 0:
            decoded = decode(data)
            self.maybe_err_ttb(decoded)
            return decoded
        else:
            return None

    def maybe_err_ttb(self, err_ttb):
        resp_a = err_ttb[0]
        if resp_a == rpberrorresp_a:
            errmsg = err_ttb[1]
            raise RiakError(bytes_to_str(errmsg))

    def maybe_riak_error(self, msg_code, data=None):
        pass

    def encode_to_ts_cell(self, cell):
        if cell is None:
            return tscell_empty
        else:
            if isinstance(cell, datetime.datetime):
                ts = unix_time_millis(cell)
                return (tscell_a, udef_a, udef_a, ts, udef_a, udef_a)
            elif isinstance(cell, bool):
                return (tscell_a, udef_a, udef_a, udef_a, cell, udef_a)
            elif isinstance(cell, six.text_type) or \
                    isinstance(cell, six.binary_type) or \
                    isinstance(cell, six.string_types):
                return (tscell_a, cell,
                        udef_a, udef_a, udef_a, udef_a)
            elif (isinstance(cell, six.integer_types)):
                return (tscell_a, udef_a, cell, udef_a, udef_a, udef_a)
            elif isinstance(cell, float):
                return (tscell_a, udef_a, udef_a, udef_a, udef_a, cell)
            else:
                t = type(cell)
                raise RiakError("can't serialize type '{}', value '{}'"
                                .format(t, cell))

    def encode_timeseries_keyreq(self, table, key, is_delete=False):
        key_vals = None
        if isinstance(key, list):
            key_vals = key
        else:
            raise ValueError("key must be a list")

        mc = MSG_CODE_TS_TTB
        rc = MSG_CODE_TS_TTB
        req_atom = tsgetreq_a
        if is_delete:
            req_atom = tsdelreq_a

        # TODO RTS-842 timeout is last
        req = req_atom, table.name, \
            [self.encode_to_ts_cell(k) for k in key_vals], udef_a
        return Msg(mc, encode(req), rc)

    def validate_timeseries_put_resp(self, resp_code, resp):
        if resp is None and \
           resp_code == riak.pb.messages.MSG_CODE_TS_PUT_RESP:
            return True
        if resp is not None:
            return True
        else:
            raise RiakError("missing response object")

    def encode_timeseries_put(self, tsobj):
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
                    req_r.append(self.encode_to_ts_cell(cell))
                req_t = (tsrow_a, req_r)
                req_rows.append(req_t)
            req = tsputreq_a, tsobj.table.name, [], req_rows
            mc = MSG_CODE_TS_TTB
            rc = MSG_CODE_TS_TTB
            return Msg(mc, encode(req), rc)
        else:
            raise RiakError("TsObject requires a list of rows")

    def decode_timeseries(self, resp_ttb, tsobj):
        """
        Fills an TsObject with the appropriate data and
        metadata from a TTB-encoded TsGetResp / TsQueryResp.

        :param resp_ttb: the decoded TTB data
        :type resp_ttb: TTB-encoded tsqueryrsp or tsgetresp
        :param tsobj: a TsObject
        :type tsobj: TsObject
        """
        # TODO TODO RTS-842 CLIENTS-814 GH-445
        # TODO COLUMNS
        # TODO TODO RTS-842 CLIENTS-814 GH-445
        # if tsobj.columns is not None:
        #     for col in resp.columns:
        #         col_name = bytes_to_str(col.name)
        #         col_type = col.type
        #         col = (col_name, col_type)
        #         tsobj.columns.append(col)
        #
        # TODO RTS-842 is this correct?
        if resp_ttb is None:
            return tsobj

        resp_a = resp_ttb[0]
        if resp_a == rpberrorresp_a:
            self.process_err_ttb(resp_ttb)
        elif resp_a == tsgetresp_a:
            # TODO resp_cols = resp_ttb[1]
            resp_rows = resp_ttb[2]
            for row_ttb in resp_rows:
                tsobj.rows.append(
                    self.decode_timeseries_row(row_ttb, None))
        else:
            raise RiakError("Unknown TTB response type: {}".format(resp_a))

    def decode_timeseries_row(self, tsrow_ttb, tscols=None):
        """
        Decodes a TTB-encoded TsRow into a list

        :param tsrow: the TTB-encoded TsRow to decode.
        :type tsrow: TTB encoded row
        :param tscols: the TTB-encoded TsColumn data to help decode.
        :type tscols: list
        :rtype list
        """
        if tsrow_ttb[0] == tsrow_a:
            row = []
            for tsc_ttb in tsrow_ttb[1]:
                if tsc_ttb[0] == tscell_a:
                    if tsc_ttb[1] != udef_a:
                        row.append(tsc_ttb[1])
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
                        "Expected tscell atom, got: {}".format(tsc_ttb[0]))
        else:
            raise RiakError(
                "Expected tsrow atom, got: {}".format(tsrow_ttb[0]))
        return row
