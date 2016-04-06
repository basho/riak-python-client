import collections
import riak.pb.messages
from riak import RiakError

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

    def maybe_riak_error(self, err_code, msg_code, data=None):
        if msg_code == err_code:
            if data is None:
                raise RiakError('no error provided!')
            return data
        else:
            return None
