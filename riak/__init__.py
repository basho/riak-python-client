"""
The Riak API for Python allows you to connect to a Riak instance,
create, modify, and delete Riak objects, add and remove links from
Riak objects, run Javascript (and Erlang) based Map/Reduce
operations, and run Linkwalking operations.
"""

from riak.riak_error import RiakError, ConflictError
from riak.client import RiakClient
from riak.bucket import RiakBucket, BucketType
from riak.table import Table
from riak.node import RiakNode
from riak.riak_object import RiakObject
from riak.mapreduce import RiakKeyFilter, RiakMapReduce, RiakLink


__all__ = ['RiakBucket', 'Table', 'BucketType', 'RiakNode',
           'RiakObject', 'RiakClient', 'RiakMapReduce', 'RiakKeyFilter',
           'RiakLink', 'RiakError', 'ConflictError',
           'ONE', 'ALL', 'QUORUM', 'key_filter']

ONE = "one"
ALL = "all"
QUORUM = "quorum"

key_filter = RiakKeyFilter()
