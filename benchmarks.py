import sys

try:
    from gevent import monkey
    monkey.patch_all()
    monkey.patch_socket(aggressive=True, dns=True)
    monkey.patch_select(aggressive=True)
except ImportError as e:
    sys.stderr.write(str(e))
    sys.stderr.write('\n')

import riak
import riak.benchmark as benchmark

from multiprocessing import cpu_count

nodes = [
    ('riak-test', 10018, 10017),
    ('riak-test', 10028, 10027),
    ('riak-test', 10038, 10037),
    ('riak-test', 10048, 10047),
    ('riak-test', 10058, 10057),
]

pool_sz = 128
if len(sys.argv) > 1:
    pool_sz = int(sys.argv[1])

iterations = 10000
if len(sys.argv) > 2:
    iterations = int(sys.argv[2])

client = riak.RiakClient(
    protocol='pbc',
    nodes=nodes,
    multiget_pool_size=pool_sz,
    multiput_pool_size=pool_sz)

bkeys = [('default', 'multi', str(key)) for key in range(iterations)]

data = None
with open(__file__) as f:
    data = f.read()

print('Benchmarking multiget:')
print('         CPUs: {0}'.format(cpu_count()))
print('Get Pool Size: {0}'.format(client._multiget_pool._size))
print('Put Pool Size: {0}'.format(client._multiput_pool._size))
print('    Key Count: {0}'.format(len(bkeys)))
print('')

with benchmark.measure() as b:
    with b.report('populate-multi'):
        objs = []
        for _, bucket_name, key in bkeys:
            bucket = client.bucket(bucket_name)
            obj = riak.RiakObject(client, bucket, key)
            obj.content_type = 'text/plain'
            obj.encoded_data = data
            objs.append(obj)
        client.multiput(objs)

for b in benchmark.measure_with_rehearsal():
    with b.report('pbc seq'):
        for _, bucket, key in bkeys:
            client.bucket(bucket).get(key)
    with b.report('pbc multi'):
        client.multiget(bkeys)
