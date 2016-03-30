import binascii
import os

import riak.benchmark as benchmark

from riak import RiakClient
from multiprocessing import cpu_count

nodes = [
    ('riak-test', 8098, 8087),
    # ('riak-test', 10018, 10017),
    # ('riak-test', 10028, 10027),
    # ('riak-test', 10038, 10037),
    # ('riak-test', 10048, 10047),
    # ('riak-test', 10058, 10057),
]
client = RiakClient(
        nodes=nodes,
        protocol='pbc',
        multiget_pool_size=128)

bkeys = [('default', 'multiget', str(key)) for key in range(10000)]

data = binascii.b2a_hex(os.urandom(1024))

print("Benchmarking multiget:")
print("      CPUs: {0}".format(cpu_count()))
print("   Threads: {0}".format(client._multiget_pool._size))
print("      Keys: {0}".format(len(bkeys)))
print()

with benchmark.measure() as b:
    with b.report('populate'):
        for _, bucket, key in bkeys:
            client.bucket(bucket).new(key, encoded_data=data,
                                      content_type='text/plain'
                                      ).store()
for b in benchmark.measure_with_rehearsal():
    # client.protocol = 'http'
    # with b.report('http seq'):
    #     for _, bucket, key in bkeys:
    #         client.bucket(bucket).get(key)
    # with b.report('http multi'):
    #     client.multiget(bkeys)

    client.protocol = 'pbc'
    with b.report('pbc seq'):
        for _, bucket, key in bkeys:
            client.bucket(bucket).get(key)
    with b.report('pbc multi'):
        client.multiget(bkeys)
