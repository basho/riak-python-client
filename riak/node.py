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
import math
import time
from threading import RLock
from riak.util import deprecated


class Decaying(object):
    """
    A float value which decays exponentially toward 0 over time. This
    is used internally to select nodes for new connections that have
    had the least errors within the recent period.
    """

    def __init__(self, p=0.0, e=math.e, r=None):
        """
        Creates a new decaying error counter.

        :param p: the initial value (defaults to 0.0)
        :type p: float
        :param e: the exponent base (defaults to math.e)
        :type e: float
        :param r: timescale factor (defaults to decaying 50% over 10
            seconds, i.e. log(0.5) / 10)
        :type r: float
        """
        self.p = p
        self.e = e
        self.r = r or (math.log(0.5) / 10)
        self.lock = RLock()
        self.t0 = time.time()

    def incr(self, d):
        """
        Increases the value by the argument.

        :param d: the value to increase by
        :type d: float
        """
        with self.lock:
            self.p = self.value() + d

    def value(self):
        """
        Returns the current value (adjusted for the time decay)

        :rtype: float
        """
        with self.lock:
            now = time.time()
            dt = now - self.t0
            self.t0 = now
            self.p = self.p * (math.pow(self.e, self.r * dt))
            return self.p


class RiakNode(object):
    """
    The internal representation of a Riak node to which the client can
    connect. Encapsulates both the configuration for the node and
    error tracking used for node-selection.
    """

    def __init__(self, host='127.0.0.1', http_port=8098, pb_port=8087,
                 **unused_args):
        """
        Creates a node.

        :param host: an IP address or hostname
        :type host: string
        :param http_port: the HTTP port of the node
        :type http_port: integer
        :param pb_port: the Protcol Buffers port of the node
        :type pb_port: integer
        """

        if 'port' in unused_args and not 'already_warned_port' in unused_args:
            deprecated("port option is deprecated, use http_port or pb_port")

        self.host = host
        self.http_port = http_port
        self.pb_port = pb_port
        self.error_rate = Decaying()
