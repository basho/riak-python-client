"""
Copyright 2012 Basho Technologies, Inc.

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
import riak_pb
from riak import RiakError
from riak.content import RiakContent
from riak.util import decode_index_value, str_to_bytes, bytes_to_str
from riak.multidict import MultiDict
from six import string_types, PY2


def _invert(d):
    out = {}
    for key in d:
        value = d[key]
        out[value] = key
    return out

REPL_TO_PY = {riak_pb.RpbBucketProps.FALSE: False,
              riak_pb.RpbBucketProps.TRUE: True,
              riak_pb.RpbBucketProps.REALTIME: 'realtime',
              riak_pb.RpbBucketProps.FULLSYNC: 'fullsync'}

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
                'notfound_ok', 'search', 'backend', 'search_index', 'datatype']
COMMIT_HOOK_PROPS = ['precommit', 'postcommit']
MODFUN_PROPS = ['chash_keyfun', 'linkfun']
QUORUM_PROPS = ['r', 'pr', 'w', 'pw', 'dw', 'rw']

MAP_FIELD_TYPES = {
    riak_pb.MapField.COUNTER: 'counter',
    riak_pb.MapField.SET: 'set',
    riak_pb.MapField.REGISTER: 'register',
    riak_pb.MapField.FLAG: 'flag',
    riak_pb.MapField.MAP: 'map',
    'counter': riak_pb.MapField.COUNTER,
    'set': riak_pb.MapField.SET,
    'register': riak_pb.MapField.REGISTER,
    'flag': riak_pb.MapField.FLAG,
    'map': riak_pb.MapField.MAP
}

DT_FETCH_TYPES = {
    riak_pb.DtFetchResp.COUNTER: 'counter',
    riak_pb.DtFetchResp.SET: 'set',
    riak_pb.DtFetchResp.MAP: 'map'
}


class RiakPbcCodec(object):
    """
    Protobuffs Encoding and decoding methods for RiakPbcTransport.
    """

    def __init__(self, **unused_args):
        if riak_pb is None:
            raise NotImplementedError("this transport is not available")
        super(RiakPbcCodec, self).__init__(**unused_args)

    def _encode_quorum(self, rw):
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

    def _decode_quorum(self, rw):
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

    def _decode_contents(self, contents, obj):
        """
        Decodes the list of siblings from the protobuf representation
        into the object.

        :param contents: a list of RpbContent messages
        :type contents: list
        :param obj: a RiakObject
        :type obj: RiakObject
        :rtype RiakObject
        """
        obj.siblings = [self._decode_content(c, RiakContent(obj))
                        for c in contents]
        # Invoke sibling-resolution logic
        if len(obj.siblings) > 1 and obj.resolver is not None:
            obj.resolver(obj)
        return obj

    def _decode_content(self, rpb_content, sibling):
        """
        Decodes a single sibling from the protobuf representation into
        a RiakObject.

        :param rpb_content: a single RpbContent message
        :type rpb_content: riak_pb.RpbContent
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

        sibling.links = [self._decode_link(link)
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

    def _encode_content(self, robj, rpb_content):
        """
        Fills an RpbContent message with the appropriate data and
        metadata from a RiakObject.

        :param robj: a RiakObject
        :type robj: RiakObject
        :param rpb_content: the protobuf message to fill
        :type rpb_content: riak_pb.RpbContent
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
        if PY2:
            rpb_content.value = str(robj.encoded_data)
        else:
            rpb_content.value = robj.encoded_data

    def _decode_link(self, link):
        """
        Decodes an RpbLink message into a tuple

        :param link: an RpbLink message
        :type link: riak_pb.RpbLink
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

    def _decode_index_value(self, index, value):
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

    def _encode_bucket_props(self, props, msg):
        """
        Encodes a dict of bucket properties into the protobuf message.

        :param props: bucket properties
        :type props: dict
        :param msg: the protobuf message to fill
        :type msg: riak_pb.RpbSetBucketReq
        """
        for prop in NORMAL_PROPS:
            if prop in props and props[prop] is not None:
                if isinstance(props[prop], string_types):
                    setattr(msg.props, prop, str_to_bytes(props[prop]))
                else:
                    setattr(msg.props, prop, props[prop])
        for prop in COMMIT_HOOK_PROPS:
            if prop in props:
                setattr(msg.props, 'has_' + prop, True)
                self._encode_hooklist(props[prop], getattr(msg.props, prop))
        for prop in MODFUN_PROPS:
            if prop in props and props[prop] is not None:
                self._encode_modfun(props[prop], getattr(msg.props, prop))
        for prop in QUORUM_PROPS:
            if prop in props and props[prop] not in (None, 'default'):
                value = self._encode_quorum(props[prop])
                if value is not None:
                    if isinstance(value, string_types):
                        setattr(msg.props, prop, str_to_bytes(value))
                    else:
                        setattr(msg.props, prop, value)
        if 'repl' in props:
            msg.props.repl = REPL_TO_PY[props['repl']]

        return msg

    def _decode_bucket_props(self, msg):
        """
        Decodes the protobuf bucket properties message into a dict.

        :param msg: the protobuf message to decode
        :type msg: riak_pb.RpbBucketProps
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
                props[prop] = self._decode_hooklist(getattr(msg, prop))
        for prop in MODFUN_PROPS:
            if msg.HasField(prop):
                props[prop] = self._decode_modfun(getattr(msg, prop))
        for prop in QUORUM_PROPS:
            if msg.HasField(prop):
                props[prop] = self._decode_quorum(getattr(msg, prop))
        if msg.HasField('repl'):
            props['repl'] = REPL_TO_PY[msg.repl]

        return props

    def _decode_modfun(self, modfun):
        """
        Decodes a protobuf modfun pair into a dict with 'mod' and
        'fun' keys. Used in bucket properties.

        :param modfun: the protobuf message to decode
        :type modfun: riak_pb.RpbModFun
        :rtype dict
        """
        return {'mod': bytes_to_str(modfun.module),
                'fun': bytes_to_str(modfun.function)}

    def _encode_modfun(self, props, msg=None):
        """
        Encodes a dict with 'mod' and 'fun' keys into a protobuf
        modfun pair. Used in bucket properties.

        :param props: the module/function pair
        :type props: dict
        :param msg: the protobuf message to fill
        :type msg: riak_pb.RpbModFun
        :rtype riak_pb.RpbModFun
        """
        if msg is None:
            msg = riak_pb.RpbModFun()
        msg.module = str_to_bytes(props['mod'])
        msg.function = str_to_bytes(props['fun'])
        return msg

    def _decode_hooklist(self, hooklist):
        """
        Decodes a list of protobuf commit hooks into their python
        equivalents. Used in bucket properties.

        :param hooklist: a list of protobuf commit hooks
        :type hooklist: list
        :rtype list
        """
        return [self._decode_hook(hook) for hook in hooklist]

    def _encode_hooklist(self, hooklist, msg):
        """
        Encodes a list of commit hooks into their protobuf equivalent.
        Used in bucket properties.

        :param hooklist: a list of commit hooks
        :type hooklist: list
        :param msg: a protobuf field that is a list of commit hooks
        """
        for hook in hooklist:
            pbhook = msg.add()
            self._encode_hook(hook, pbhook)

    def _decode_hook(self, hook):
        """
        Decodes a protobuf commit hook message into a dict. Used in
        bucket properties.

        :param hook: the hook to decode
        :type hook: riak_pb.RpbCommitHook
        :rtype dict
        """
        if hook.HasField('modfun'):
            return self._decode_modfun(hook.modfun)
        else:
            return {'name': bytes_to_str(hook.name)}

    def _encode_hook(self, hook, msg):
        """
        Encodes a commit hook dict into the protobuf message. Used in
        bucket properties.

        :param hook: the hook to encode
        :type hook: dict
        :param msg: the protobuf message to fill
        :type msg: riak_pb.RpbCommitHook
        :rtype riak_pb.RpbCommitHook
        """
        if 'name' in hook:
            msg.name = str_to_bytes(hook['name'])
        else:
            self._encode_modfun(hook, msg.modfun)
        return msg

    def _encode_index_req(self, bucket, index, startkey, endkey=None,
                          return_terms=None, max_results=None,
                          continuation=None, timeout=None, term_regex=None):
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
        :rtype riak_pb.RpbIndexReq
        """
        req = riak_pb.RpbIndexReq(bucket=str_to_bytes(bucket.name),
                                  index=str_to_bytes(index))
        self._add_bucket_type(req, bucket.bucket_type)
        if endkey is not None:
            req.qtype = riak_pb.RpbIndexReq.range
            req.range_min = str_to_bytes(str(startkey))
            req.range_max = str_to_bytes(str(endkey))
        else:
            req.qtype = riak_pb.RpbIndexReq.eq
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
        return req

    def _decode_search_index(self, index):
        """
        Fills an RpbYokozunaIndex message with the appropriate data.

        :param index: a yz index message
        :type index: riak_pb.RpbYokozunaIndex
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
            if not self.bucket_types():
                raise NotImplementedError(
                    'Server does not support bucket-types')
            req.type = str_to_bytes(bucket_type.name)

    def _encode_search_query(self, req, params):
        if 'rows' in params:
            req.rows = params['rows']
        if 'start' in params:
            req.start = params['start']
        if 'sort' in params:
            req.sort = str_to_bytes(params['sort'])
        if 'filter' in params:
            req.filter = str_to_bytes(params['filter'])
        if 'df' in params:
            req.df = str_to_bytes(params['df'])
        if 'op' in params:
            req.op = str_to_bytes(params['op'])
        if 'q.op' in params:
            req.op = params['q.op']
        if 'fl' in params:
            if isinstance(params['fl'], list):
                req.fl.extend(params['fl'])
            else:
                req.fl.append(params['fl'])
        if 'presort' in params:
            req.presort = params['presort']

    def _decode_search_doc(self, doc):
        resultdoc = MultiDict()
        for pair in doc.fields:
            if PY2:
                ukey = unicode(pair.key, 'utf-8')
                uval = unicode(pair.value, 'utf-8')
            else:
                ukey = bytes_to_str(pair.key)
                uval = bytes_to_str(pair.value)
            resultdoc.add(ukey, uval)
        return resultdoc.mixed()

    def _decode_dt_fetch(self, resp):
        dtype = DT_FETCH_TYPES.get(resp.type)
        if dtype is None:
            raise ValueError("Unknown datatype on wire: {}".format(resp.type))

        value = self._decode_dt_value(dtype, resp.value)

        if resp.HasField('context'):
            context = resp.context[:]
        else:
            context = None

        return dtype, value, context

    def _decode_dt_value(self, dtype, msg):
        if dtype == 'counter':
            return msg.counter_value
        elif dtype == 'set':
            return self._decode_set_value(msg.set_value)
        elif dtype == 'map':
            return self._decode_map_value(msg.map_value)

    def _encode_dt_options(self, req, params):
        for q in ['r', 'pr', 'w', 'dw', 'pw']:
            if q in params and params[q] is not None:
                setattr(req, q, self._encode_quorum(params[q]))

        for o in ['basic_quorum', 'notfound_ok', 'timeout', 'return_body',
                  'include_context']:
            if o in params and params[o] is not None:
                setattr(req, o, params[o])

    def _decode_map_value(self, entries):
        out = {}
        for entry in entries:
            name = bytes_to_str(entry.field.name[:])
            dtype = MAP_FIELD_TYPES[entry.field.type]
            if dtype == 'counter':
                value = entry.counter_value
            elif dtype == 'set':
                value = self._decode_set_value(entry.set_value)
            elif dtype == 'register':
                value = bytes_to_str(entry.register_value[:])
            elif dtype == 'flag':
                value = entry.flag_value
            elif dtype == 'map':
                value = self._decode_map_value(entry.map_value)
            out[(name, dtype)] = value
        return out

    def _decode_set_value(self, set_value):
        return [bytes_to_str(string[:]) for string in set_value]

    def _encode_dt_op(self, dtype, req, op):
        if dtype == 'counter':
            req.op.counter_op.increment = op[1]
        elif dtype == 'set':
            self._encode_set_op(req.op, op)
        elif dtype == 'map':
            self._encode_map_op(req.op.map_op, op)
        else:
            raise TypeError("Cannot send operation on datatype {!r}".
                            format(dtype))

    def _encode_set_op(self, msg, op):
        if 'adds' in op:
            msg.set_op.adds.extend(str_to_bytes(op['adds']))
        if 'removes' in op:
            msg.set_op.removes.extend(str_to_bytes(op['removes']))

    def _encode_map_op(self, msg, ops):
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
                self._encode_map_update(dtype, update, op[2])

    def _encode_map_update(self, dtype, msg, op):
        if dtype == 'counter':
            # ('increment', some_int)
            msg.counter_op.increment = op[1]
        elif dtype == 'set':
            self._encode_set_op(msg, op)
        elif dtype == 'map':
            self._encode_map_op(msg.map_op, op)
        elif dtype == 'register':
            # ('assign', some_str)
            msg.register_op = str_to_bytes(op[1])
        elif dtype == 'flag':
            if op == 'enable':
                msg.flag_op = riak_pb.MapUpdate.ENABLE
            else:
                msg.flag_op = riak_pb.MapUpdate.DISABLE
