import collections

import riak.pb.messages

from riak import RiakError
from riak.codecs.util import parse_pbuf_msg
from riak.util import bytes_to_str

Msg = collections.namedtuple('Msg',
                             ['msg_code', 'data', 'resp_code'],
                             verbose=False)


class Codec(object):
    def parse_msg(self):
        raise NotImplementedError('parse_msg not implemented')

    def maybe_incorrect_code(self, resp_code, expect=None):
        if expect and resp_code != expect:
            raise RiakError("unexpected message code: %d, expected %d"
                            % (resp_code, expect))

    def maybe_riak_error(self, msg_code, data=None):
        if msg_code == riak.pb.messages.MSG_CODE_ERROR_RESP:
            if data is None:
                raise RiakError('no error provided!')
            else:
                err = parse_pbuf_msg(msg_code, data)
                raise RiakError(bytes_to_str(err.errmsg))
