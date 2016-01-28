from riak import RiakClient
from multiprocessing import cpu_count
import binascii
import os
import riak.benchmark as benchmark
import riak.client.multiget as mget

client = RiakClient(protocol='pbc')
bkeys = [('default', 'multiget', str(key)) for key in range(10000)]

data = binascii.b2a_hex(os.urandom(1024))

print("Benchmarking multiget:")
print("      CPUs: {0}".format(cpu_count()))
print("   Threads: {0}".format(mget.POOL_SIZE))
print("      Keys: {0}".format(len(bkeys)))
print()

with benchmark.measure() as b:
    with b.report('populate'):
        for _, bucket, key in bkeys:
            client.bucket(bucket).new(key, encoded_data=data,
                                        content_type='text/plain'
                                        ).store()
for b in benchmark.measure_with_rehearsal():
    client.protocol = 'http'
    with b.report('http seq'):
        for _, bucket, key in bkeys:
            client.bucket(bucket).get(key)

    with b.report('http multi'):
        mget.multiget(client, bkeys)

    client.protocol = 'pbc'
    with b.report('pbc seq'):
        for _, bucket, key in bkeys:
            client.bucket(bucket).get(key)

    with b.report('pbc multi'):
        mget.multiget(client, bkeys)
