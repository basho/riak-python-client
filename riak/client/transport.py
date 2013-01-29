"""
Copyright 2012 Basho Technologies, Inc.

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
"""
from contextlib import contextmanager
from riak.transports.pool import BadResource
from riak.transports.pbc import is_retryable as is_pbc_retryable
from riak.transports.http import is_retryable as is_http_retryable
import httplib


class RiakClientTransport(object):
    """
    Methods for RiakClient related to transport selection and retries.
    """

    RETRY_COUNT = 3

    # These will be set or redefined by the RiakClient initializer
    protocol = 'http'
    _http_pool = None
    _pb_pool = None

    @contextmanager
    def _transport(self):
        pool = self._choose_pool()
        with pool.take() as transport:
            yield transport

    def _with_retries(self, pool, fn):
        skip_nodes = []

        def _skip_bad_nodes(transport):
            return transport._node not in skip_nodes

        for retry in range(self.RETRY_COUNT):
            try:
                with pool.take(_filter=_skip_bad_nodes) as transport:
                    try:
                        return fn(transport)
                    except (IOError, httplib.HTTPException) as e:
                        if is_retryable(e):
                            transport._node.error_rate.incr(1)
                            skip_nodes.append(transport._node)
                            raise BadResource(e)
                        else:
                            raise e
            except BadResource:
                continue

    def _choose_pool(self, protocol=None):
        if not protocol:
            protocol = self.protocol
        if protocol in ['http', 'https']:
            pool = self._http_pool
        elif protocol == 'pbc':
            pool = self._pb_pool
        else:
            raise ValueError("invalid protocol %s" % protocol)
        return pool


def is_retryable(error):
    return is_pbc_retryable(error) or is_http_retryable(error)


def retryable(fn, protocol=None):
    def wrapper(self, *args, **kwargs):
        pool = self._choose_pool(protocol)

        def thunk(transport):
            return fn(self, transport, *args, **kwargs)

        return self._with_retries(pool, thunk)

    return wrapper


def retryableHttpOnly(fn):
    return retryable(fn, protocol='http')
