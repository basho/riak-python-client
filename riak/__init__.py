"""
Copyright 2010 Rusty Klophaus <rusty@basho.com>
Copyright 2010 Justin Sheehy <justin@basho.com>
Copyright 2009 Jay Baird <jay@mochimedia.com>

This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
---
The Riak API for Python allows you to connect to a Riak instance,
create, modify, and delete Riak objects, add and remove links from
Riak objects, run Javascript (and Erlang) based Map/Reduce
operations, and run Linkwalking operations.

See the unit_tests.py file for example usage.

@author Rusty Klophaus (@rklophaus) (rusty@basho.com)
@author Andy Gross (@argv0) (andy@basho.com)
@author Jon Meredith (@jmeredith) (jmeredith@basho.com)
@author Jay Baird (@skatterbean) (jay@mochimedia.com)
"""


class RiakError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

from riak_object import RiakObject
from bucket import RiakBucket
from client import RiakClient
from mapreduce import RiakMapReduce, RiakMapReducePhase, RiakLinkPhase,\
    RiakKeyFilter
from transports.pbc import RiakPbcTransport
from transports.http import RiakHttpTransport

ONE = "one"
ALL = "all"
QUORUM = "quorum"

key_filter = RiakKeyFilter()
