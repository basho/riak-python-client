import datetime
import six

from erlastic import encode, decode
from erlastic.types import Atom

from riak import RiakError
from riak.codecs import Codec, Msg
from riak.pb.messages import MSG_CODE_TS_TTB_MSG
from riak.ts_object import TsColumns
from riak.util import bytes_to_str, unix_time_millis, \
    datetime_from_unix_time_millis

udef_a = Atom('undefined')

rpberrorresp_a = Atom('rpberrorresp')
tsgetreq_a = Atom('tsgetreq')
tsgetresp_a = Atom('tsgetresp')
tsqueryreq_a = Atom('tsqueryreq')
tsqueryresp_a = Atom('tsqueryresp')
tsinterpolation_a = Atom('tsinterpolation')
tsputreq_a = Atom('tsputreq')
tsputresp_a = Atom('tsputresp')
tsdelreq_a = Atom('tsdelreq')
timestamp_a = Atom('timestamp')


class TtbCodec(Codec):
    '''
    Erlang term-to-binary Encoding and decoding methods for TcpTransport
    '''

    def __init__(self, **unused_args):
        super(TtbCodec, self).__init__(**unused_args)

    def parse_msg(self, msg_code, data):
        if msg_code != MSG_CODE_TS_TTB_MSG:
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
            # errcode = err_ttb[2]
            raise RiakError(bytes_to_str(errmsg))

    def encode_to_ts_cell(self, cell):
        if cell is None:
            return []
        else:
            if isinstance(cell, datetime.datetime):
                ts = unix_time_millis(cell)
                # logging.debug('encoded datetime %s as %s', cell, ts)
                return ts
            elif isinstance(cell, bool):
                return cell
            elif isinstance(cell, six.text_type) or \
                    isinstance(cell, six.binary_type) or \
                    isinstance(cell, six.string_types):
                return cell
            elif (isinstance(cell, six.integer_types)):
                return cell
            elif isinstance(cell, float):
                return cell
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

        mc = MSG_CODE_TS_TTB_MSG
        rc = MSG_CODE_TS_TTB_MSG
        req_atom = tsgetreq_a
        if is_delete:
            req_atom = tsdelreq_a

        # TODO FUTURE add timeout as last param
        req = req_atom, table.name, \
            [self.encode_to_ts_cell(k) for k in key_vals], udef_a
        return Msg(mc, encode(req), rc)

    def validate_timeseries_put_resp(self, resp_code, resp):
        if resp is None and resp_code == MSG_CODE_TS_TTB_MSG:
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
            raise NotImplementedError('columns are not used')

        if tsobj.rows and isinstance(tsobj.rows, list):
            req_rows = []
            for row in tsobj.rows:
                req_r = []
                for cell in row:
                    req_r.append(self.encode_to_ts_cell(cell))
                req_rows.append(tuple(req_r))
            req = tsputreq_a, tsobj.table.name, [], req_rows
            mc = MSG_CODE_TS_TTB_MSG
            rc = MSG_CODE_TS_TTB_MSG
            return Msg(mc, encode(req), rc)
        else:
            raise RiakError("TsObject requires a list of rows")

    def encode_timeseries_query(self, table, query, interpolations=None):
        q = query
        if '{table}' in q:
            q = q.format(table=table.name)
        tsi = tsinterpolation_a, q, []
        req = tsqueryreq_a, tsi, False, udef_a
        mc = MSG_CODE_TS_TTB_MSG
        rc = MSG_CODE_TS_TTB_MSG
        return Msg(mc, encode(req), rc)

    def decode_timeseries(self, resp_ttb, tsobj,
                          convert_timestamp=False):
        """
        Fills an TsObject with the appropriate data and
        metadata from a TTB-encoded TsGetResp / TsQueryResp.

        :param resp_ttb: the decoded TTB data
        :type resp_ttb: TTB-encoded tsqueryrsp or tsgetresp
        :param tsobj: a TsObject
        :type tsobj: TsObject
        :param convert_timestamp: Convert timestamps to datetime objects
        :type tsobj: boolean
        """
        if resp_ttb is None:
            return tsobj

        self.maybe_err_ttb(resp_ttb)

        # NB: some queries return a BARE 'tsqueryresp' atom
        # catch that here:
        if resp_ttb == tsqueryresp_a:
            return tsobj

        # The response atom is the first element in the response tuple
        resp_a = resp_ttb[0]
        if resp_a == tsputresp_a:
            return
        elif resp_a == tsgetresp_a or resp_a == tsqueryresp_a:
            resp_data = resp_ttb[1]
            if len(resp_data) == 0:
                return
            elif len(resp_data) == 3:
                resp_colnames = resp_data[0]
                resp_coltypes = resp_data[1]
                tsobj.columns = self.decode_timeseries_cols(
                        resp_colnames, resp_coltypes)
                resp_rows = resp_data[2]
                tsobj.rows = []
                for resp_row in resp_rows:
                    tsobj.rows.append(
                        self.decode_timeseries_row(resp_row, resp_coltypes,
                                                   convert_timestamp))
            else:
                raise RiakError(
                    "Expected 3-tuple in response, got: {}".format(resp_data))
        else:
            raise RiakError("Unknown TTB response type: {}".format(resp_a))

    def decode_timeseries_cols(self, cnames, ctypes):
        cnames = [bytes_to_str(cname) for cname in cnames]
        ctypes = [str(ctype) for ctype in ctypes]
        return TsColumns(cnames, ctypes)

    def decode_timeseries_row(self, tsrow, tsct, convert_timestamp=False):
        """
        Decodes a TTB-encoded TsRow into a list

        :param tsrow: the TTB decoded TsRow to decode.
        :type tsrow: TTB dncoded row
        :param tsct: the TTB decoded column types (atoms).
        :type tsct: list
        :param convert_timestamp: Convert timestamps to datetime objects
        :type tsobj: boolean
        :rtype list
        """
        row = []
        for i, cell in enumerate(tsrow):
            if cell is None:
                row.append(None)
            elif isinstance(cell, list) and len(cell) == 0:
                row.append(None)
            else:
                if convert_timestamp and tsct[i] == timestamp_a:
                    row.append(datetime_from_unix_time_millis(cell))
                else:
                    row.append(cell)
        return row
