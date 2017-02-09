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

import logging
import riak

from riak import RiakError
from riak.tests import RUN_YZ
from riak.tests.base import IntegrationTestBase


def yzSetUp(*yzdata):
    if RUN_YZ:
        riak.disable_list_exceptions = True
        c = IntegrationTestBase.create_client()
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
        c.close()


def yzTearDown(c, *yzdata):
    if RUN_YZ:
        riak.disable_list_exceptions = True
        c = IntegrationTestBase.create_client()
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
        c.close()
        riak.disable_list_exceptions = False
