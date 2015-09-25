"""
Copyright 2015 Basho Technologies, Inc.

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

from riak.datatypes.datatype import Datatype
from riak.datatypes import TYPES


class Counter(Datatype):
    """
    A convergent datatype that represents a counter which can be
    incremented or decremented. This type can stand on its own or be
    embedded within a :class:`~riak.datatypes.Map`.
    """

    type_name = 'counter'
    _type_error_msg = "Counters can only be integers"

    def _post_init(self):
        self._increment = 0

    def _default_value(self):
        return 0

    @Datatype.modified.getter
    def modified(self):
        """
        Whether this counter has staged increments.
        """
        return self._increment is not 0

    def to_op(self):
        """
        Extracts the mutation operation from the counter
        :rtype: int, None
        """
        if not self._increment == 0:
            return ('increment', self._increment)

    def increment(self, amount=1):
        """
        Increments the counter by one or the given amount.

        :param amount: the amount to increment the counter
        :type amount: int
        """
        self._raise_if_badtype(amount)
        self._increment += amount

    def decrement(self, amount=1):
        """
        Decrements the counter by one or the given amount.

        :param amount: the amount to decrement the counter
        :type amount: int
        """
        self._raise_if_badtype(amount)
        self._increment -= amount

    def _check_type(self, new_value):
        return (isinstance(new_value, int) or
                isinstance(new_value, long))  # noqa


TYPES['counter'] = Counter
