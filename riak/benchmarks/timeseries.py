# Copyright 2010-present Basho Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import random
import sys

import riak.benchmark as benchmark

from multiprocessing import cpu_count
from riak import RiakClient

# logger = logging.getLogger()
# logger.level = logging.DEBUG
# logger.addHandler(logging.StreamHandler(sys.stdout))

# batch sizes 8, 16, 32, 64, 128, 256
if len(sys.argv) != 3:
    raise AssertionError(
            'first arg is batch size, second arg is true / false'
            'for use_ttb')

rowcount = 32768
batchsz = int(sys.argv[1])
if rowcount % batchsz != 0:
    raise AssertionError('rowcount must be divisible by batchsz')
use_ttb = sys.argv[2].lower() == 'true'

epoch = datetime.datetime.utcfromtimestamp(0)
onesec = datetime.timedelta(0, 1)

weather = ['typhoon', 'hurricane', 'rain', 'wind', 'snow']
rows = []
for i in range(rowcount):
    ts = datetime.datetime(2016, 1, 1, 12, 0, 0) + \
        datetime.timedelta(seconds=i)
    family_idx = i % batchsz
    series_idx = i % batchsz
    family = 'hash{:d}'.format(family_idx)
    series = 'user{:d}'.format(series_idx)
    w = weather[i % len(weather)]
    temp = (i % 100) + random.random()
    row = [family, series, ts, w, temp]
    key = [family, series, ts]
    rows.append(row)

print("Benchmarking timeseries:")
print("   Use TTB: {}".format(use_ttb))
print("Batch Size: {}".format(batchsz))
print("      CPUs: {}".format(cpu_count()))
print("      Rows: {}".format(len(rows)))
print()

tbl = 'GeoCheckin'
h = 'riak-test'
n = [
    {'host': h, 'pb_port': 10017},
    {'host': h, 'pb_port': 10027},
    {'host': h, 'pb_port': 10037},
    {'host': h, 'pb_port': 10047},
    {'host': h, 'pb_port': 10057}
]
client = RiakClient(nodes=n, protocol='pbc',
                    transport_options={'use_ttb': use_ttb})
table = client.table(tbl)

with benchmark.measure() as b:
    for i in (1, 2, 3):
        with b.report('populate-%d' % i):
            for i in range(0, rowcount, batchsz):
                x = i
                y = i + batchsz
                r = rows[x:y]
                ts_obj = table.new(r)
                result = ts_obj.store()
                if result is not True:
                    raise AssertionError("expected success")
