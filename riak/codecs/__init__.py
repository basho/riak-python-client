import collections

Msg = collections.namedtuple('Msg',
        ['msg_code', 'data', 'resp_code'], verbose=False)
