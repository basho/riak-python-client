import logging

from riak import RiakClient, RiakError
from riak.tests import RUN_YZ, PROTOCOL, HOST, PB_PORT, HTTP_PORT, SECURITY_CREDS

def yzSetUp(*yzdata):
    if RUN_YZ:
        c = RiakClient(protocol=PROTOCOL, host=HOST, http_port=HTTP_PORT,
                        pb_port=PB_PORT, credentials=SECURITY_CREDS)
        for yz in yzdata:
            logging.debug("yzSetUp: %s", yz)
            c.create_search_index(yz['index'], timeout=30000)
            if yz['btype'] is not None:
                t = c.bucket_type(yz['btype'])
                b = t.bucket(yz['bucket'])
            else:
                b = c.bucket(yz['bucket'])
            # Keep trying to set search bucket property until it succeeds
            index_set = False
            while not index_set:
                try:
                    b.set_property('search_index', yz['index'])
                    index_set = True
                except RiakError:
                    pass

def yzTearDown(c, *yzdata):
    if RUN_YZ:
        c = RiakClient(protocol=PROTOCOL, host=HOST, http_port=HTTP_PORT,
                        pb_port=PB_PORT, credentials=SECURITY_CREDS)
        for yz in yzdata:
            logging.debug("yzTearDown: %s", yz)
            if yz['btype'] is not None:
                t = c.bucket_type(yz['btype'])
                b = t.bucket(yz['bucket'])
            else:
                b = c.bucket(yz['bucket'])
            b.set_property('search_index', '_dont_index_')
            c.delete_search_index(yz['index'])
            for keys in b.stream_keys():
                for key in keys:
                    b.delete(key)
