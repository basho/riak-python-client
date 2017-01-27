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

"""
The Riak API for Python allows you to connect to a Riak instance,
create, modify, and delete Riak objects, add and remove links from
Riak objects, run Javascript (and Erlang) based Map/Reduce
operations, and run Linkwalking operations.
"""

from riak.riak_error import RiakError, ConflictError, ListError
from riak.client import RiakClient
from riak.bucket import RiakBucket, BucketType
from riak.table import Table
from riak.node import RiakNode
from riak.riak_object import RiakObject
from riak.mapreduce import RiakKeyFilter, RiakMapReduce, RiakLink


__all__ = ['RiakBucket', 'Table', 'BucketType', 'RiakNode',
           'RiakObject', 'RiakClient', 'RiakMapReduce', 'RiakKeyFilter',
           'RiakLink', 'RiakError', 'ConflictError', 'ListError',
           'ONE', 'ALL', 'QUORUM', 'key_filter',
           'disable_list_exceptions']

ONE = "one"
ALL = "all"
QUORUM = "quorum"

key_filter = RiakKeyFilter()

"""
Set to true to allow listing operations
"""
disable_list_exceptions = False
