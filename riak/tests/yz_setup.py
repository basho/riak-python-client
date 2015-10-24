from riak import RiakError

def yzSetUpModule(c, *yzdata):
    for yz in yzdata:
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

def yzTearDownModule(c, *yzdata):
    for yz in yzdata:
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
