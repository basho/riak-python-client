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

import six

from .datatype import Datatype
from riak.datatypes import TYPES

__all__ = ['Hll']


class Hll(Datatype):
    """A convergent datatype representing a HyperLogLog set.
    Currently strings are the only supported value type.
    Example::

        myhll.add('barista')
        myhll.add('roaster')
        myhll.add('brewer')
    """

    type_name = 'hll'
    _type_error_msg = 'Hlls can only be integers'

    def _post_init(self):
        self._adds = set()

    def _default_value(self):
        return 0

    @Datatype.modified.getter
    def modified(self):
        """
        Whether this HyperLogLog has staged adds.
        """
        return len(self._adds) > 0

    def to_op(self):
        """
        Extracts the modification operation from the Hll.

        :rtype: dict, None
        """
        if not self._adds:
            return None
        changes = {}
        if self._adds:
            changes['adds'] = list(self._adds)
        return changes

    def add(self, element):
        """
        Adds an element to the HyperLogLog. Datatype cardinality will
        be updated when the object is saved.

        :param element: the element to add
        :type element: str
        """
        if not isinstance(element, six.string_types):
            raise TypeError("Hll elements can only be strings")
        self._adds.add(element)

    def _coerce_value(self, new_value):
        return int(new_value)

    def _check_type(self, new_value):
        return isinstance(new_value, six.integer_types)


TYPES['hll'] = Hll
