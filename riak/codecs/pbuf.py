import datetime
import six

import riak.pb.messages
import riak.pb.riak_pb2
import riak.pb.riak_dt_pb2
import riak.pb.riak_kv_pb2
import riak.pb.riak_ts_pb2

from riak import RiakError
from riak.codecs import Codec, Msg
from riak.codecs.util import parse_pbuf_msg
from riak.content import RiakContent
from riak.pb.riak_ts_pb2 import TsColumnType
from riak.riak_object import VClock
from riak.ts_object import TsColumns
from riak.util import decode_index_value, str_to_bytes, bytes_to_str, \
    unix_time_millis, datetime_from_unix_time_millis
from riak.multidict import MultiDict


def _invert(d):
    out = {}
    for key in d:
        value = d[key]
        out[value] = key
    return out


REPL_TO_PY = {
    riak.pb.riak_pb2.RpbBucketProps.FALSE: False,
    riak.pb.riak_pb2.RpbBucketProps.TRUE: True,
    riak.pb.riak_pb2.RpbBucketProps.REALTIME: 'realtime',
    riak.pb.riak_pb2.RpbBucketProps.FULLSYNC: 'fullsync'
}

REPL_TO_PB = _invert(REPL_TO_PY)

RIAKC_RW_ONE = 4294967294
RIAKC_RW_QUORUM = 4294967293
RIAKC_RW_ALL = 4294967292
RIAKC_RW_DEFAULT = 4294967291

QUORUM_TO_PB = {'default': RIAKC_RW_DEFAULT,
                'all': RIAKC_RW_ALL,
                'quorum': RIAKC_RW_QUORUM,
                'one': RIAKC_RW_ONE}

QUORUM_TO_PY = _invert(QUORUM_TO_PB)

NORMAL_PROPS = ['n_val', 'allow_mult', 'last_write_wins', 'old_vclock',
                'young_vclock', 'big_vclock', 'small_vclock', 'basic_quorum',
                'notfound_ok', 'search', 'backend', 'search_index', 'datatype',
                'write_once', 'hll_precision']
COMMIT_HOOK_PROPS = ['precommit', 'postcommit']
MODFUN_PROPS = ['chash_keyfun', 'linkfun']
QUORUM_PROPS = ['r', 'pr', 'w', 'pw', 'dw', 'rw']

MAP_FIELD_TYPES = {
    riak.pb.riak_dt_pb2.MapField.COUNTER: 'counter',
    riak.pb.riak_dt_pb2.MapField.SET: 'set',
    riak.pb.riak_dt_pb2.MapField.REGISTER: 'register',
    riak.pb.riak_dt_pb2.MapField.FLAG: 'flag',
    riak.pb.riak_dt_pb2.MapField.MAP: 'map',
    'counter': riak.pb.riak_dt_pb2.MapField.COUNTER,
    'set': riak.pb.riak_dt_pb2.MapField.SET,
    'register': riak.pb.riak_dt_pb2.MapField.REGISTER,
    'flag': riak.pb.riak_dt_pb2.MapField.FLAG,
    'map': riak.pb.riak_dt_pb2.MapField.MAP
}

DT_FETCH_TYPES = {
    riak.pb.riak_dt_pb2.DtFetchResp.COUNTER: 'counter',
    riak.pb.riak_dt_pb2.DtFetchResp.SET: 'set',
    riak.pb.riak_dt_pb2.DtFetchResp.MAP: 'map',
    riak.pb.riak_dt_pb2.DtFetchResp.HLL: 'hll'
}


class PbufCodec(Codec):
    '''
    Protobuffs Encoding and decoding methods for TcpTransport.
    '''

    def __init__(self,
                 client_timeouts=False, quorum_controls=False,
                 tombstone_vclocks=False, bucket_types=False):
        if riak.pb is None:
            raise NotImplementedError("this codec is not available")
        self._client_timeouts = client_timeouts
        self._quorum_controls = quorum_controls
        self._tombstone_vclocks = tombstone_vclocks
        self._bucket_types = bucket_types

    def parse_msg(self, msg_code, data):
        return parse_pbuf_msg(msg_code, data)

    def encode_auth(self, username, password):
        req = riak.pb.riak_pb2.RpbAuthReq()
        req.user = str_to_bytes(username)
        req.password = str_to_bytes(password)
        mc = riak.pb.messages.MSG_CODE_AUTH_REQ
        rc = riak.pb.messages.MSG_CODE_AUTH_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def encode_ping(self):
        return Msg(riak.pb.messages.MSG_CODE_PING_REQ, None,
                   riak.pb.messages.MSG_CODE_PING_RESP)

    def encode_quorum(self, rw):
        """
        Converts a symbolic quorum value into its on-the-wire
        equivalent.

        :param rw: the quorum
        :type rw: string, integer
        :rtype: integer
        """
        if rw in QUORUM_TO_PB:
            return QUORUM_TO_PB[rw]
        elif type(rw) is int and rw >= 0:
            return rw
        else:
            return None

    def decode_quorum(self, rw):
        """
        Converts a protobuf quorum value to a symbolic value if
        necessary.

        :param rw: the quorum
        :type rw: int
        :rtype int or string
        """
        if rw in QUORUM_TO_PY:
            return QUORUM_TO_PY[rw]
        else:
            return rw

    def decode_contents(self, contents, obj):
        """
        Decodes the list of siblings from the protobuf representation
        into the object.

        :param contents: a list of RpbContent messages
        :type contents: list
        :param obj: a RiakObject
        :type obj: RiakObject
        :rtype RiakObject
        """
        obj.siblings = [self.decode_content(c, RiakContent(obj))
                        for c in contents]
        # Invoke sibling-resolution logic
        if len(obj.siblings) > 1 and obj.resolver is not None:
            obj.resolver(obj)
        return obj

    def decode_content(self, rpb_content, sibling):
        """
        Decodes a single sibling from the protobuf representation into
        a RiakObject.

        :param rpb_content: a single RpbContent message
        :type rpb_content: riak.pb.riak_pb2.RpbContent
        :param sibling: a RiakContent sibling container
        :type sibling: RiakContent
        :rtype: RiakContent
        """

        if rpb_content.HasField("deleted") and rpb_content.deleted:
            sibling.exists = False
        else:
            sibling.exists = True
        if rpb_content.HasField("content_type"):
            sibling.content_type = bytes_to_str(rpb_content.content_type)
        if rpb_content.HasField("charset"):
            sibling.charset = bytes_to_str(rpb_content.charset)
        if rpb_content.HasField("content_encoding"):
            sibling.content_encoding = \
                bytes_to_str(rpb_content.content_encoding)
        if rpb_content.HasField("vtag"):
            sibling.etag = bytes_to_str(rpb_content.vtag)

        sibling.links = [self.decode_link(link)
                         for link in rpb_content.links]
        if rpb_content.HasField("last_mod"):
            sibling.last_modified = float(rpb_content.last_mod)
            if rpb_content.HasField("last_mod_usecs"):
                sibling.last_modified += rpb_content.last_mod_usecs / 1000000.0

        sibling.usermeta = dict([(bytes_to_str(usermd.key),
                                  bytes_to_str(usermd.value))
                                 for usermd in rpb_content.usermeta])
        sibling.indexes = set([(bytes_to_str(index.key),
                                decode_index_value(index.key, index.value))
                               for index in rpb_content.indexes])
        sibling.encoded_data = rpb_content.value

        return sibling

    def encode_content(self, robj, rpb_content):
        """
        Fills an RpbContent message with the appropriate data and
        metadata from a RiakObject.

        :param robj: a RiakObject
        :type robj: RiakObject
        :param rpb_content: the protobuf message to fill
        :type rpb_content: riak.pb.riak_pb2.RpbContent
        """
        if robj.content_type:
            rpb_content.content_type = str_to_bytes(robj.content_type)
        if robj.charset:
            rpb_content.charset = str_to_bytes(robj.charset)
        if robj.content_encoding:
            rpb_content.content_encoding = str_to_bytes(robj.content_encoding)
        for uk in robj.usermeta:
            pair = rpb_content.usermeta.add()
            pair.key = str_to_bytes(uk)
            pair.value = str_to_bytes(robj.usermeta[uk])
        for link in robj.links:
            pb_link = rpb_content.links.add()
            try:
                bucket, key, tag = link
            except ValueError:
                raise RiakError("Invalid link tuple %s" % link)

            pb_link.bucket = str_to_bytes(bucket)
            pb_link.key = str_to_bytes(key)
            if tag:
                pb_link.tag = str_to_bytes(tag)
            else:
                pb_link.tag = str_to_bytes('')

        for field, value in robj.indexes:
            pair = rpb_content.indexes.add()
            pair.key = str_to_bytes(field)
            pair.value = str_to_bytes(str(value))

        # Python 2.x data is stored in a string
        if six.PY2:
            rpb_content.value = str(robj.encoded_data)
        else:
            rpb_content.value = robj.encoded_data

    def decode_link(self, link):
        """
        Decodes an RpbLink message into a tuple

        :param link: an RpbLink message
        :type link: riak.pb.riak_pb2.RpbLink
        :rtype tuple
        """

        if link.HasField("bucket"):
            bucket = bytes_to_str(link.bucket)
        else:
            bucket = None
        if link.HasField("key"):
            key = bytes_to_str(link.key)
        else:
            key = None
        if link.HasField("tag"):
            tag = bytes_to_str(link.tag)
        else:
            tag = None

        return (bucket, key, tag)

    def decode_index_value(self, index, value):
        """
        Decodes a secondary index value into the correct Python type.
        :param index: the name of the index
        :type index: str
        :param value: the value of the index entry
        :type  value: str
        :rtype str or int
        """
        if index.endswith("_int"):
            return int(value)
        else:
            return bytes_to_str(value)

    def encode_bucket_props(self, props, msg):
        """
        Encodes a dict of bucket properties into the protobuf message.

        :param props: bucket properties
        :type props: dict
        :param msg: the protobuf message to fill
        :type msg: riak.pb.riak_pb2.RpbSetBucketReq
        """
        for prop in NORMAL_PROPS:
            if prop in props and props[prop] is not None:
                if isinstance(props[prop], six.string_types):
                    setattr(msg.props, prop, str_to_bytes(props[prop]))
                else:
                    setattr(msg.props, prop, props[prop])
        for prop in COMMIT_HOOK_PROPS:
            if prop in props:
                setattr(msg.props, 'has_' + prop, True)
                self.encode_hooklist(props[prop], getattr(msg.props, prop))
        for prop in MODFUN_PROPS:
            if prop in props and props[prop] is not None:
                self.encode_modfun(props[prop], getattr(msg.props, prop))
        for prop in QUORUM_PROPS:
            if prop in props and props[prop] not in (None, 'default'):
                value = self.encode_quorum(props[prop])
                if value is not None:
                    if isinstance(value, six.string_types):
                        setattr(msg.props, prop, str_to_bytes(value))
                    else:
                        setattr(msg.props, prop, value)
        if 'repl' in props:
            msg.props.repl = REPL_TO_PB[props['repl']]

        return msg

    def decode_bucket_props(self, msg):
        """
        Decodes the protobuf bucket properties message into a dict.

        :param msg: the protobuf message to decode
        :type msg: riak.pb.riak_pb2.RpbBucketProps
        :rtype dict
        """
        props = {}
        for prop in NORMAL_PROPS:
            if msg.HasField(prop):
                props[prop] = getattr(msg, prop)
                if isinstance(props[prop], bytes):
                    props[prop] = bytes_to_str(props[prop])
        for prop in COMMIT_HOOK_PROPS:
            if getattr(msg, 'has_' + prop):
                props[prop] = self.decode_hooklist(getattr(msg, prop))
        for prop in MODFUN_PROPS:
            if msg.HasField(prop):
                props[prop] = self.decode_modfun(getattr(msg, prop))
        for prop in QUORUM_PROPS:
            if msg.HasField(prop):
                props[prop] = self.decode_quorum(getattr(msg, prop))
        if msg.HasField('repl'):
            props['repl'] = REPL_TO_PY[msg.repl]
        return props

    def decode_modfun(self, modfun):
        """
        Decodes a protobuf modfun pair into a dict with 'mod' and
        'fun' keys. Used in bucket properties.

        :param modfun: the protobuf message to decode
        :type modfun: riak.pb.riak_pb2.RpbModFun
        :rtype dict
        """
        return {'mod': bytes_to_str(modfun.module),
                'fun': bytes_to_str(modfun.function)}

    def encode_modfun(self, props, msg=None):
        """
        Encodes a dict with 'mod' and 'fun' keys into a protobuf
        modfun pair. Used in bucket properties.

        :param props: the module/function pair
        :type props: dict
        :param msg: the protobuf message to fill
        :type msg: riak.pb.riak_pb2.RpbModFun
        :rtype riak.pb.riak_pb2.RpbModFun
        """
        if msg is None:
            msg = riak.pb.riak_pb2.RpbModFun()
        msg.module = str_to_bytes(props['mod'])
        msg.function = str_to_bytes(props['fun'])
        return msg

    def decode_hooklist(self, hooklist):
        """
        Decodes a list of protobuf commit hooks into their python
        equivalents. Used in bucket properties.

        :param hooklist: a list of protobuf commit hooks
        :type hooklist: list
        :rtype list
        """
        return [self.decode_hook(hook) for hook in hooklist]

    def encode_hooklist(self, hooklist, msg):
        """
        Encodes a list of commit hooks into their protobuf equivalent.
        Used in bucket properties.

        :param hooklist: a list of commit hooks
        :type hooklist: list
        :param msg: a protobuf field that is a list of commit hooks
        """
        for hook in hooklist:
            pbhook = msg.add()
            self.encode_hook(hook, pbhook)

    def decode_hook(self, hook):
        """
        Decodes a protobuf commit hook message into a dict. Used in
        bucket properties.

        :param hook: the hook to decode
        :type hook: riak.pb.riak_pb2.RpbCommitHook
        :rtype dict
        """
        if hook.HasField('modfun'):
            return self.decode_modfun(hook.modfun)
        else:
            return {'name': bytes_to_str(hook.name)}

    def encode_hook(self, hook, msg):
        """
        Encodes a commit hook dict into the protobuf message. Used in
        bucket properties.

        :param hook: the hook to encode
        :type hook: dict
        :param msg: the protobuf message to fill
        :type msg: riak.pb.riak_pb2.RpbCommitHook
        :rtype riak.pb.riak_pb2.RpbCommitHook
        """
        if 'name' in hook:
            msg.name = str_to_bytes(hook['name'])
        else:
            self.encode_modfun(hook, msg.modfun)
        return msg

    def encode_index_req(self, bucket, index, startkey, endkey=None,
                         return_terms=None, max_results=None,
                         continuation=None, timeout=None, term_regex=None,
                         streaming=False):
        """
        Encodes a secondary index request into the protobuf message.

        :param bucket: the bucket whose index to query
        :type bucket: string
        :param index: the index to query
        :type index: string
        :param startkey: the value or beginning of the range
        :type startkey: integer, string
        :param endkey: the end of the range
        :type endkey: integer, string
        :param return_terms: whether to return the index term with the key
        :type return_terms: bool
        :param max_results: the maximum number of results to return (page size)
        :type max_results: integer
        :param continuation: the opaque continuation returned from a
            previous paginated request
        :type continuation: string
        :param timeout: a timeout value in milliseconds, or 'infinity'
        :type timeout: int
        :param term_regex: a regular expression used to filter index terms
        :type term_regex: string
        :param streaming: encode as streaming request
        :type streaming: bool
        :rtype riak.pb.riak_kv_pb2.RpbIndexReq
        """
        req = riak.pb.riak_kv_pb2.RpbIndexReq(
            bucket=str_to_bytes(bucket.name),
            index=str_to_bytes(index))
        self._add_bucket_type(req, bucket.bucket_type)
        if endkey is not None:
            req.qtype = riak.pb.riak_kv_pb2.RpbIndexReq.range
            req.range_min = str_to_bytes(str(startkey))
            req.range_max = str_to_bytes(str(endkey))
        else:
            req.qtype = riak.pb.riak_kv_pb2.RpbIndexReq.eq
            req.key = str_to_bytes(str(startkey))
        if return_terms is not None:
            req.return_terms = return_terms
        if max_results:
            req.max_results = max_results
        if continuation:
            req.continuation = str_to_bytes(continuation)
        if timeout:
            if timeout == 'infinity':
                req.timeout = 0
            else:
                req.timeout = timeout
        if term_regex:
            req.term_regex = str_to_bytes(term_regex)
        req.stream = streaming
        mc = riak.pb.messages.MSG_CODE_INDEX_REQ
        rc = riak.pb.messages.MSG_CODE_INDEX_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def decode_index_req(self, resp, index,
                         return_terms=None, max_results=None):
        if return_terms and resp.results:
            results = [(decode_index_value(index, pair.key),
                        bytes_to_str(pair.value))
                       for pair in resp.results]
        else:
            results = resp.keys[:]
            if six.PY3:
                results = [bytes_to_str(key) for key in resp.keys]

        if max_results is not None and resp.HasField('continuation'):
            return (results, bytes_to_str(resp.continuation))
        else:
            return (results, None)

    def decode_search_index(self, index):
        """
        Fills an RpbYokozunaIndex message with the appropriate data.

        :param index: a yz index message
        :type index: riak.pb.riak_yokozuna_pb2.RpbYokozunaIndex
        :rtype dict
        """
        result = {}
        result['name'] = bytes_to_str(index.name)
        if index.HasField('schema'):
            result['schema'] = bytes_to_str(index.schema)
        if index.HasField('n_val'):
            result['n_val'] = index.n_val
        return result

    def _add_bucket_type(self, req, bucket_type):
        if bucket_type and not bucket_type.is_default():
            if not self._bucket_types:
                raise NotImplementedError(
                    'Server does not support bucket-types')
            req.type = str_to_bytes(bucket_type.name)

    def encode_search_query(self, req, **kwargs):
        if 'rows' in kwargs:
            req.rows = kwargs['rows']
        if 'start' in kwargs:
            req.start = kwargs['start']
        if 'sort' in kwargs:
            req.sort = str_to_bytes(kwargs['sort'])
        if 'filter' in kwargs:
            req.filter = str_to_bytes(kwargs['filter'])
        if 'df' in kwargs:
            req.df = str_to_bytes(kwargs['df'])
        if 'op' in kwargs:
            req.op = str_to_bytes(kwargs['op'])
        if 'q.op' in kwargs:
            req.op = kwargs['q.op']
        if 'fl' in kwargs:
            if isinstance(kwargs['fl'], list):
                req.fl.extend(kwargs['fl'])
            else:
                req.fl.append(kwargs['fl'])
        if 'presort' in kwargs:
            req.presort = kwargs['presort']

    def decode_search_doc(self, doc):
        resultdoc = MultiDict()
        for pair in doc.fields:
            if six.PY2:
                ukey = unicode(pair.key, 'utf-8')    # noqa
                uval = unicode(pair.value, 'utf-8')  # noqa
            else:
                ukey = bytes_to_str(pair.key)
                uval = bytes_to_str(pair.value)
            resultdoc.add(ukey, uval)
        return resultdoc.mixed()

    def decode_dt_fetch(self, resp):
        dtype = DT_FETCH_TYPES.get(resp.type)
        if dtype is None:
            raise ValueError("Unknown datatype on wire: {}".format(resp.type))

        value = self.decode_dt_value(dtype, resp.value)

        if resp.HasField('context'):
            context = resp.context[:]
        else:
            context = None

        return dtype, value, context

    def decode_dt_value(self, dtype, msg):
        if dtype == 'counter':
            return msg.counter_value
        elif dtype == 'set':
            return self.decode_set_value(msg.set_value)
        elif dtype == 'hll':
            return self.decode_hll_value(msg.hll_value)
        elif dtype == 'map':
            return self.decode_map_value(msg.map_value)

    def encode_dt_options(self, req, **kwargs):
        for q in ['r', 'pr', 'w', 'dw', 'pw']:
            if q in kwargs and kwargs[q] is not None:
                setattr(req, q, self.encode_quorum(kwargs[q]))

        for o in ['basic_quorum', 'notfound_ok', 'timeout', 'return_body',
                  'include_context']:
            if o in kwargs and kwargs[o] is not None:
                setattr(req, o, kwargs[o])

    def decode_map_value(self, entries):
        out = {}
        for entry in entries:
            name = bytes_to_str(entry.field.name[:])
            dtype = MAP_FIELD_TYPES[entry.field.type]
            if dtype == 'counter':
                value = entry.counter_value
            elif dtype == 'set':
                value = self.decode_set_value(entry.set_value)
            elif dtype == 'register':
                value = bytes_to_str(entry.register_value[:])
            elif dtype == 'flag':
                value = entry.flag_value
            elif dtype == 'map':
                value = self.decode_map_value(entry.map_value)
            else:
                raise ValueError(
                    'Map may not contain datatype: {}'
                    .format(dtype))
            out[(name, dtype)] = value
        return out

    def decode_set_value(self, set_value):
        return [bytes_to_str(string[:]) for string in set_value]

    def decode_hll_value(self, hll_value):
        return int(hll_value)

    def encode_dt_op(self, dtype, req, op):
        if dtype == 'counter':
            req.op.counter_op.increment = op[1]
        elif dtype == 'set':
            self.encode_set_op(req.op, op)
        elif dtype == 'hll':
            self.encode_hll_op(req.op, op)
        elif dtype == 'map':
            self.encode_map_op(req.op.map_op, op)
        else:
            raise TypeError("Cannot send operation on datatype {!r}".
                            format(dtype))

    def encode_set_op(self, msg, op):
        if 'adds' in op:
            msg.set_op.adds.extend(str_to_bytes(op['adds']))
        if 'removes' in op:
            msg.set_op.removes.extend(str_to_bytes(op['removes']))

    def encode_hll_op(self, msg, op):
        if 'adds' in op:
            msg.hll_op.adds.extend(str_to_bytes(op['adds']))

    def encode_map_op(self, msg, ops):
        for op in ops:
            name, dtype = op[1]
            ftype = MAP_FIELD_TYPES[dtype]
            if op[0] == 'add':
                add = msg.adds.add()
                add.name = str_to_bytes(name)
                add.type = ftype
            elif op[0] == 'remove':
                remove = msg.removes.add()
                remove.name = str_to_bytes(name)
                remove.type = ftype
            elif op[0] == 'update':
                update = msg.updates.add()
                update.field.name = str_to_bytes(name)
                update.field.type = ftype
                self.encode_map_update(dtype, update, op[2])

    def encode_map_update(self, dtype, msg, op):
        if dtype == 'counter':
            # ('increment', some_int)
            msg.counter_op.increment = op[1]
        elif dtype == 'set':
            self.encode_set_op(msg, op)
        elif dtype == 'map':
            self.encode_map_op(msg.map_op, op)
        elif dtype == 'register':
            # ('assign', some_str)
            msg.register_op = str_to_bytes(op[1])
        elif dtype == 'flag':
            if op == 'enable':
                msg.flag_op = riak.pb.riak_dt_pb2.MapUpdate.ENABLE
            else:
                msg.flag_op = riak.pb.riak_dt_pb2.MapUpdate.DISABLE
        else:
            raise ValueError(
                'Map may not contain datatype: {}'
                .format(dtype))

    def encode_to_ts_cell(self, cell, ts_cell):
        if cell is not None:
            if isinstance(cell, datetime.datetime):
                ts_cell.timestamp_value = unix_time_millis(cell)
            elif isinstance(cell, bool):
                ts_cell.boolean_value = cell
            elif isinstance(cell, six.binary_type):
                ts_cell.varchar_value = cell
            elif isinstance(cell, six.text_type):
                ts_cell.varchar_value = str_to_bytes(cell)
            elif isinstance(cell, six.string_types):
                ts_cell.varchar_value = str_to_bytes(cell)
            elif (isinstance(cell, six.integer_types)):
                ts_cell.sint64_value = cell
            elif isinstance(cell, float):
                ts_cell.double_value = cell
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

        req = riak.pb.riak_ts_pb2.TsGetReq()
        mc = riak.pb.messages.MSG_CODE_TS_GET_REQ
        rc = riak.pb.messages.MSG_CODE_TS_GET_RESP
        if is_delete:
            req = riak.pb.riak_ts_pb2.TsDelReq()
            mc = riak.pb.messages.MSG_CODE_TS_DEL_REQ
            rc = riak.pb.messages.MSG_CODE_TS_DEL_RESP

        req.table = str_to_bytes(table.name)
        for cell in key_vals:
            ts_cell = req.key.add()
            self.encode_to_ts_cell(cell, ts_cell)
        return Msg(mc, req.SerializeToString(), rc)

    def encode_timeseries_listkeysreq(self, table, timeout=None):
        req = riak.pb.riak_ts_pb2.TsListKeysReq()
        req.table = str_to_bytes(table.name)
        if self._client_timeouts and timeout:
            req.timeout = timeout
        mc = riak.pb.messages.MSG_CODE_TS_LIST_KEYS_REQ
        rc = riak.pb.messages.MSG_CODE_TS_LIST_KEYS_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def validate_timeseries_put_resp(self, resp_code, resp):
        if resp is not None:
            return True
        else:
            raise RiakError("missing response object")

    def encode_timeseries_put(self, tsobj):
        """
        Fills an TsPutReq message with the appropriate data and
        metadata from a TsObject.

        :param tsobj: a TsObject
        :type tsobj: TsObject
        :param req: the protobuf message to fill
        :type req: riak.pb.riak_ts_pb2.TsPutReq
        """
        req = riak.pb.riak_ts_pb2.TsPutReq()
        req.table = str_to_bytes(tsobj.table.name)

        if tsobj.columns:
            raise NotImplementedError("columns are not implemented yet")

        if tsobj.rows and isinstance(tsobj.rows, list):
            for row in tsobj.rows:
                tsr = req.rows.add()  # NB: type TsRow
                if not isinstance(row, list):
                    raise ValueError("TsObject row must be a list of values")
                for cell in row:
                    tsc = tsr.cells.add()  # NB: type TsCell
                    self.encode_to_ts_cell(cell, tsc)
        else:
            raise RiakError("TsObject requires a list of rows")

        mc = riak.pb.messages.MSG_CODE_TS_PUT_REQ
        rc = riak.pb.messages.MSG_CODE_TS_PUT_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def encode_timeseries_query(self, table, query, interpolations=None):
        req = riak.pb.riak_ts_pb2.TsQueryReq()
        q = query
        if '{table}' in q:
            q = q.format(table=table.name)
        req.query.base = str_to_bytes(q)
        mc = riak.pb.messages.MSG_CODE_TS_QUERY_REQ
        rc = riak.pb.messages.MSG_CODE_TS_QUERY_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def decode_timeseries(self, resp, tsobj,
                          convert_timestamp=False):
        """
        Fills an TsObject with the appropriate data and
        metadata from a TsGetResp / TsQueryResp.

        :param resp: the protobuf message from which to process data
        :type resp: riak.pb.riak_ts_pb2.TsQueryRsp or
                    riak.pb.riak_ts_pb2.TsGetResp
        :param tsobj: a TsObject
        :type tsobj: TsObject
        :param convert_timestamp: Convert timestamps to datetime objects
        :type tsobj: boolean
        """
        if resp.columns is not None:
            col_names = []
            col_types = []
            for col in resp.columns:
                col_names.append(bytes_to_str(col.name))
                col_type = self.decode_timeseries_col_type(col.type)
                col_types.append(col_type)
            tsobj.columns = TsColumns(col_names, col_types)

        tsobj.rows = []
        if resp.rows is not None:
            for row in resp.rows:
                tsobj.rows.append(
                    self.decode_timeseries_row(
                        row, resp.columns, convert_timestamp))

    def decode_timeseries_col_type(self, col_type):
        # NB: these match the atom names for column types
        if col_type == TsColumnType.Value('VARCHAR'):
            return 'varchar'
        elif col_type == TsColumnType.Value('SINT64'):
            return 'sint64'
        elif col_type == TsColumnType.Value('DOUBLE'):
            return 'double'
        elif col_type == TsColumnType.Value('TIMESTAMP'):
            return 'timestamp'
        elif col_type == TsColumnType.Value('BOOLEAN'):
            return 'boolean'
        elif col_type == TsColumnType.Value('BLOB'):
            return 'blob'
        else:
            msg = 'could not decode column type: {}'.format(col_type)
            raise RiakError(msg)

    def decode_timeseries_row(self, tsrow, tscols=None,
                              convert_timestamp=False):
        """
        Decodes a TsRow into a list

        :param tsrow: the protobuf TsRow to decode.
        :type tsrow: riak.pb.riak_ts_pb2.TsRow
        :param tscols: the protobuf TsColumn data to help decode.
        :type tscols: list
        :rtype list
        """
        row = []
        for i, cell in enumerate(tsrow.cells):
            col = None
            if tscols is not None:
                col = tscols[i]
            if cell.HasField('varchar_value'):
                if col and not (col.type == TsColumnType.Value('VARCHAR') or
                                col.type == TsColumnType.Value('BLOB')):
                    raise TypeError('expected VARCHAR or BLOB column')
                else:
                    row.append(cell.varchar_value)
            elif cell.HasField('sint64_value'):
                if col and col.type != TsColumnType.Value('SINT64'):
                    raise TypeError('expected SINT64 column')
                else:
                    row.append(cell.sint64_value)
            elif cell.HasField('double_value'):
                if col and col.type != TsColumnType.Value('DOUBLE'):
                    raise TypeError('expected DOUBLE column')
                else:
                    row.append(cell.double_value)
            elif cell.HasField('timestamp_value'):
                if col and col.type != TsColumnType.Value('TIMESTAMP'):
                    raise TypeError('expected TIMESTAMP column')
                else:
                    dt = cell.timestamp_value
                    if convert_timestamp:
                        dt = datetime_from_unix_time_millis(
                            cell.timestamp_value)
                    row.append(dt)
            elif cell.HasField('boolean_value'):
                if col and col.type != TsColumnType.Value('BOOLEAN'):
                    raise TypeError('expected BOOLEAN column')
                else:
                    row.append(cell.boolean_value)
            else:
                row.append(None)
        return row

    def decode_preflist(self, item):
        """
        Decodes a preflist response

        :param preflist: a bucket/key preflist
        :type preflist: list of
                        riak.pb.riak_kv_pb2.RpbBucketKeyPreflistItem
        :rtype dict
        """
        result = {'partition': item.partition,
                  'node': bytes_to_str(item.node),
                  'primary': item. primary}
        return result

    def encode_get(self, robj, r=None, pr=None, timeout=None,
                   basic_quorum=None, notfound_ok=None,
                   head_only=False):
        bucket = robj.bucket
        req = riak.pb.riak_kv_pb2.RpbGetReq()
        if r:
            req.r = self.encode_quorum(r)
        if self._quorum_controls:
            if pr:
                req.pr = self.encode_quorum(pr)
            if basic_quorum is not None:
                req.basic_quorum = basic_quorum
            if notfound_ok is not None:
                req.notfound_ok = notfound_ok
        if self._client_timeouts and timeout:
            req.timeout = timeout
        if self._tombstone_vclocks:
            req.deletedvclock = True
        req.bucket = str_to_bytes(bucket.name)
        self._add_bucket_type(req, bucket.bucket_type)
        req.key = str_to_bytes(robj.key)
        req.head = head_only
        mc = riak.pb.messages.MSG_CODE_GET_REQ
        rc = riak.pb.messages.MSG_CODE_GET_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def encode_put(self, robj, w=None, dw=None, pw=None,
                   return_body=True, if_none_match=False,
                   timeout=None):
        bucket = robj.bucket
        req = riak.pb.riak_kv_pb2.RpbPutReq()
        if w:
            req.w = self.encode_quorum(w)
        if dw:
            req.dw = self.encode_quorum(dw)
        if self._quorum_controls and pw:
            req.pw = self.encode_quorum(pw)
        if return_body:
            req.return_body = 1
        if if_none_match:
            req.if_none_match = 1
        if self._client_timeouts and timeout:
            req.timeout = timeout
        req.bucket = str_to_bytes(bucket.name)
        self._add_bucket_type(req, bucket.bucket_type)
        if robj.key:
            req.key = str_to_bytes(robj.key)
        if robj.vclock:
            req.vclock = robj.vclock.encode('binary')
        self.encode_content(robj, req.content)
        mc = riak.pb.messages.MSG_CODE_PUT_REQ
        rc = riak.pb.messages.MSG_CODE_PUT_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def decode_get(self, robj, resp):
        if resp is not None:
            if resp.HasField('vclock'):
                robj.vclock = VClock(resp.vclock, 'binary')
            # We should do this even if there are no contents, i.e.
            # the object is tombstoned
            self.decode_contents(resp.content, robj)
        else:
            # "not found" returns an empty message,
            # so let's make sure to clear the siblings
            robj.siblings = []
        return robj

    def decode_put(self, robj, resp):
        if resp is not None:
            if resp.HasField('key'):
                robj.key = bytes_to_str(resp.key)
            if resp.HasField("vclock"):
                robj.vclock = VClock(resp.vclock, 'binary')
            if resp.content:
                self.decode_contents(resp.content, robj)
        elif not robj.key:
            raise RiakError("missing response object")
        return robj

    def encode_delete(self, robj, rw=None, r=None,
                      w=None, dw=None, pr=None, pw=None,
                      timeout=None):
        req = riak.pb.riak_kv_pb2.RpbDelReq()
        if rw:
            req.rw = self.encode_quorum(rw)
        if r:
            req.r = self.encode_quorum(r)
        if w:
            req.w = self.encode_quorum(w)
        if dw:
            req.dw = self.encode_quorum(dw)

        if self._quorum_controls:
            if pr:
                req.pr = self.encode_quorum(pr)
            if pw:
                req.pw = self.encode_quorum(pw)

        if self._client_timeouts and timeout:
            req.timeout = timeout

        use_vclocks = (self._tombstone_vclocks and
                       hasattr(robj, 'vclock') and robj.vclock)
        if use_vclocks:
            req.vclock = robj.vclock.encode('binary')

        bucket = robj.bucket
        req.bucket = str_to_bytes(bucket.name)
        self._add_bucket_type(req, bucket.bucket_type)
        req.key = str_to_bytes(robj.key)
        mc = riak.pb.messages.MSG_CODE_DEL_REQ
        rc = riak.pb.messages.MSG_CODE_DEL_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def encode_stream_keys(self, bucket, timeout=None):
        req = riak.pb.riak_kv_pb2.RpbListKeysReq()
        req.bucket = str_to_bytes(bucket.name)
        if self._client_timeouts and timeout:
            req.timeout = timeout
        self._add_bucket_type(req, bucket.bucket_type)
        mc = riak.pb.messages.MSG_CODE_LIST_KEYS_REQ
        rc = riak.pb.messages.MSG_CODE_LIST_KEYS_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def decode_get_keys(self, stream):
        keys = []
        for keylist in stream:
            for key in keylist:
                keys.append(bytes_to_str(key))
        return keys

    def decode_get_server_info(self, resp):
        return {'node': bytes_to_str(resp.node),
                'server_version': bytes_to_str(resp.server_version)}

    def encode_get_client_id(self):
        mc = riak.pb.messages.MSG_CODE_GET_CLIENT_ID_REQ
        rc = riak.pb.messages.MSG_CODE_GET_CLIENT_ID_RESP
        return Msg(mc, None, rc)

    def decode_get_client_id(self, resp):
        return bytes_to_str(resp.client_id)

    def encode_set_client_id(self, client_id):
        req = riak.pb.riak_kv_pb2.RpbSetClientIdReq()
        req.client_id = str_to_bytes(client_id)
        mc = riak.pb.messages.MSG_CODE_SET_CLIENT_ID_REQ
        rc = riak.pb.messages.MSG_CODE_SET_CLIENT_ID_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def encode_get_buckets(self, bucket_type,
                           timeout=None, streaming=False):
        # Bucket streaming landed in the same release as timeouts, so
        # we don't need to check the capability.
        req = riak.pb.riak_kv_pb2.RpbListBucketsReq()
        req.stream = streaming
        self._add_bucket_type(req, bucket_type)
        if self._client_timeouts and timeout:
            req.timeout = timeout
        mc = riak.pb.messages.MSG_CODE_LIST_BUCKETS_REQ
        rc = riak.pb.messages.MSG_CODE_LIST_BUCKETS_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def encode_get_bucket_props(self, bucket):
        req = riak.pb.riak_pb2.RpbGetBucketReq()
        req.bucket = str_to_bytes(bucket.name)
        self._add_bucket_type(req, bucket.bucket_type)
        mc = riak.pb.messages.MSG_CODE_GET_BUCKET_REQ
        rc = riak.pb.messages.MSG_CODE_GET_BUCKET_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def encode_set_bucket_props(self, bucket, props):
        req = riak.pb.riak_pb2.RpbSetBucketReq()
        req.bucket = str_to_bytes(bucket.name)
        self._add_bucket_type(req, bucket.bucket_type)
        self.encode_bucket_props(props, req)
        mc = riak.pb.messages.MSG_CODE_SET_BUCKET_REQ
        rc = riak.pb.messages.MSG_CODE_SET_BUCKET_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def encode_clear_bucket_props(self, bucket):
        req = riak.pb.riak_pb2.RpbResetBucketReq()
        req.bucket = str_to_bytes(bucket.name)
        self._add_bucket_type(req, bucket.bucket_type)
        mc = riak.pb.messages.MSG_CODE_RESET_BUCKET_REQ
        rc = riak.pb.messages.MSG_CODE_RESET_BUCKET_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def encode_get_bucket_type_props(self, bucket_type):
        req = riak.pb.riak_pb2.RpbGetBucketTypeReq()
        req.type = str_to_bytes(bucket_type.name)
        mc = riak.pb.messages.MSG_CODE_GET_BUCKET_TYPE_REQ
        rc = riak.pb.messages.MSG_CODE_GET_BUCKET_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def encode_set_bucket_type_props(self, bucket_type, props):
        req = riak.pb.riak_pb2.RpbSetBucketTypeReq()
        req.type = str_to_bytes(bucket_type.name)
        self.encode_bucket_props(props, req)
        mc = riak.pb.messages.MSG_CODE_SET_BUCKET_TYPE_REQ
        rc = riak.pb.messages.MSG_CODE_SET_BUCKET_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def encode_stream_mapred(self, content):
        req = riak.pb.riak_kv_pb2.RpbMapRedReq()
        req.request = str_to_bytes(content)
        req.content_type = str_to_bytes("application/json")
        mc = riak.pb.messages.MSG_CODE_MAP_RED_REQ
        rc = riak.pb.messages.MSG_CODE_MAP_RED_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def encode_create_search_index(self, index, schema=None,
                                   n_val=None, timeout=None):
        index = str_to_bytes(index)
        idx = riak.pb.riak_yokozuna_pb2.RpbYokozunaIndex(name=index)
        if schema:
            idx.schema = str_to_bytes(schema)
        if n_val:
            idx.n_val = n_val
        req = riak.pb.riak_yokozuna_pb2.RpbYokozunaIndexPutReq(index=idx)
        if timeout is not None:
            req.timeout = timeout
        mc = riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_PUT_REQ
        rc = riak.pb.messages.MSG_CODE_PUT_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def encode_get_search_index(self, index):
        req = riak.pb.riak_yokozuna_pb2.RpbYokozunaIndexGetReq(
                name=str_to_bytes(index))
        mc = riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_GET_REQ
        rc = riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_GET_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def encode_list_search_indexes(self):
        req = riak.pb.riak_yokozuna_pb2.RpbYokozunaIndexGetReq()
        mc = riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_GET_REQ
        rc = riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_GET_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def encode_delete_search_index(self, index):
        req = riak.pb.riak_yokozuna_pb2.RpbYokozunaIndexDeleteReq(
                name=str_to_bytes(index))
        mc = riak.pb.messages.MSG_CODE_YOKOZUNA_INDEX_DELETE_REQ
        rc = riak.pb.messages.MSG_CODE_DEL_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def encode_create_search_schema(self, schema, content):
        scma = riak.pb.riak_yokozuna_pb2.RpbYokozunaSchema(
                name=str_to_bytes(schema),
                content=str_to_bytes(content))
        req = riak.pb.riak_yokozuna_pb2.RpbYokozunaSchemaPutReq(
                schema=scma)
        mc = riak.pb.messages.MSG_CODE_YOKOZUNA_SCHEMA_PUT_REQ
        rc = riak.pb.messages.MSG_CODE_PUT_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def encode_get_search_schema(self, schema):
        req = riak.pb.riak_yokozuna_pb2.RpbYokozunaSchemaGetReq(
                name=str_to_bytes(schema))
        mc = riak.pb.messages.MSG_CODE_YOKOZUNA_SCHEMA_GET_REQ
        rc = riak.pb.messages.MSG_CODE_YOKOZUNA_SCHEMA_GET_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def decode_get_search_schema(self, resp):
        result = {}
        result['name'] = bytes_to_str(resp.schema.name)
        result['content'] = bytes_to_str(resp.schema.content)
        return result

    def encode_search(self, index, query, **kwargs):
        req = riak.pb.riak_search_pb2.RpbSearchQueryReq(
                index=str_to_bytes(index),
                q=str_to_bytes(query))
        self.encode_search_query(req, **kwargs)
        mc = riak.pb.messages.MSG_CODE_SEARCH_QUERY_REQ
        rc = riak.pb.messages.MSG_CODE_SEARCH_QUERY_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def decode_search(self, resp):
        result = {}
        if resp.HasField('max_score'):
            result['max_score'] = resp.max_score
        if resp.HasField('num_found'):
            result['num_found'] = resp.num_found
        result['docs'] = [self.decode_search_doc(doc) for doc in resp.docs]
        return result

    def encode_get_counter(self, bucket, key, **kwargs):
        req = riak.pb.riak_kv_pb2.RpbCounterGetReq()
        req.bucket = str_to_bytes(bucket.name)
        req.key = str_to_bytes(key)
        if kwargs.get('r') is not None:
            req.r = self.encode_quorum(kwargs['r'])
        if kwargs.get('pr') is not None:
            req.pr = self.encode_quorum(kwargs['pr'])
        if kwargs.get('basic_quorum') is not None:
            req.basic_quorum = kwargs['basic_quorum']
        if kwargs.get('notfound_ok') is not None:
            req.notfound_ok = kwargs['notfound_ok']
        mc = riak.pb.messages.MSG_CODE_COUNTER_GET_REQ
        rc = riak.pb.messages.MSG_CODE_COUNTER_GET_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def encode_update_counter(self, bucket, key, value, **kwargs):
        req = riak.pb.riak_kv_pb2.RpbCounterUpdateReq()
        req.bucket = str_to_bytes(bucket.name)
        req.key = str_to_bytes(key)
        req.amount = value
        if kwargs.get('w') is not None:
            req.w = self.encode_quorum(kwargs['w'])
        if kwargs.get('dw') is not None:
            req.dw = self.encode_quorum(kwargs['dw'])
        if kwargs.get('pw') is not None:
            req.pw = self.encode_quorum(kwargs['pw'])
        if kwargs.get('returnvalue') is not None:
            req.returnvalue = kwargs['returnvalue']
        mc = riak.pb.messages.MSG_CODE_COUNTER_UPDATE_REQ
        rc = riak.pb.messages.MSG_CODE_COUNTER_UPDATE_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def encode_fetch_datatype(self, bucket, key, **kwargs):
        req = riak.pb.riak_dt_pb2.DtFetchReq()
        req.type = str_to_bytes(bucket.bucket_type.name)
        req.bucket = str_to_bytes(bucket.name)
        req.key = str_to_bytes(key)
        self.encode_dt_options(req, **kwargs)
        mc = riak.pb.messages.MSG_CODE_DT_FETCH_REQ
        rc = riak.pb.messages.MSG_CODE_DT_FETCH_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def encode_update_datatype(self, datatype, **kwargs):
        op = datatype.to_op()
        type_name = datatype.type_name
        if not op:
            raise ValueError("No operation to send on datatype {!r}".
                             format(datatype))
        req = riak.pb.riak_dt_pb2.DtUpdateReq()
        req.bucket = str_to_bytes(datatype.bucket.name)
        req.type = str_to_bytes(datatype.bucket.bucket_type.name)
        if datatype.key:
            req.key = str_to_bytes(datatype.key)
        if datatype._context:
            req.context = datatype._context
        self.encode_dt_options(req, **kwargs)
        self.encode_dt_op(type_name, req, op)
        mc = riak.pb.messages.MSG_CODE_DT_UPDATE_REQ
        rc = riak.pb.messages.MSG_CODE_DT_UPDATE_RESP
        return Msg(mc, req.SerializeToString(), rc)

    def decode_update_datatype(self, datatype, resp, **kwargs):
        type_name = datatype.type_name
        if resp.HasField('key'):
            datatype.key = resp.key[:]
        if resp.HasField('context'):
            datatype._context = resp.context[:]
        if kwargs.get('return_body'):
            datatype._set_value(self.decode_dt_value(type_name, resp))

    def encode_get_preflist(self, bucket, key):
        req = riak.pb.riak_kv_pb2.RpbGetBucketKeyPreflistReq()
        req.bucket = str_to_bytes(bucket.name)
        req.key = str_to_bytes(key)
        req.type = str_to_bytes(bucket.bucket_type.name)
        mc = riak.pb.messages.MSG_CODE_GET_BUCKET_KEY_PREFLIST_REQ
        rc = riak.pb.messages.MSG_CODE_GET_BUCKET_KEY_PREFLIST_RESP
        return Msg(mc, req.SerializeToString(), rc)
