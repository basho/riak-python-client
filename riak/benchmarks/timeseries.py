from multiprocessing import cpu_count
from riak import RiakClient
import riak.benchmark as benchmark
import datetime
import random

epoch = datetime.datetime.utcfromtimestamp(0)
onesec = datetime.timedelta(0, 1)

rowcount = 32768
batchsz = 32
if rowcount % batchsz != 0:
    raise AssertionError('rowcount must be divisible by batchsz')

weather = ['typhoon', 'hurricane', 'rain', 'wind', 'snow']
rows = []
keys = []
for i in range(rowcount):
    ts = datetime.datetime(2016, 1, 1, 12, 0, 0) + \
        datetime.timedelta(seconds=i)
    family_idx = i % 4
    series_idx = i % 4
    family = 'hash{:d}'.format(family_idx)
    series = 'user{:d}'.format(series_idx)
    w = weather[i % len(weather)]
    temp = (i % 100) + random.random()
    row = [family, series, ts, w, temp]
    key = [family, series, ts]
    rows.append(row)
    keys.append(key)

print("Benchmarking timeseries:")
print("      CPUs: {0}".format(cpu_count()))
print("      Rows: {0}".format(len(rows)))
print()

tbl = 'GeoCheckin'
h = 'riak-test'
n = [
    {'host': h, 'pb_port': 10017},
    {'host': h, 'pb_port': 10027},
    {'host': h, 'pb_port': 10037},
    {'host': h, 'pb_port': 10047}
]
client = RiakClient(nodes=n, protocol='pbc')
table = client.table(tbl)

with benchmark.measure() as b:
    with b.report('populate'):
        for i in range(0, rowcount, batchsz):
            x = i
            y = i + batchsz
            r = rows[x:y]
            ts_obj = table.new(r)
            result = ts_obj.store()
            if result is not True:
                raise AssertionError("expected success")
    with b.report('get'):
        for k in keys:
            ts_obj = client.ts_get(tbl, k)
            if ts_obj is None:
                raise AssertionError("expected obj")
            if len(ts_obj.rows) != 1:
                raise AssertionError("expected one row")
            row = ts_obj.rows[0]
            if len(row) != 5:
                raise AssertionError("expected row to have five items")
