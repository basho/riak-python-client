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

import collections

from .datatype import Datatype
from six import string_types
from riak.datatypes import TYPES

__all__ = ['GSet']


class GSet(collections.Set, Datatype):
    """A convergent datatype representing a GSet.
    Currently strings are the only supported value type.
    Example::

        myset.add('barista')
        myset.add('roaster')
        myset.add('brewer')

    This datatype also implements the `Set ABC
    <https://docs.python.org/2/library/collections.html>`_, meaning it
    supports ``len()``, ``in``, and iteration.

    """

    type_name = 'gset'
    _type_error_msg = "GSets can only be iterables of strings"

    def _post_init(self):
        self._adds = set()

    def _default_value(self):
        return frozenset()

    @Datatype.modified.getter
    def modified(self):
        """
        Whether this gset has staged adds.
        """
        return len(self._adds) > 0

    def to_op(self):
        """
        Extracts the modification operation from the set.

        :rtype: dict, None
        """
        if not self._adds:
            return None
        changes = {}
        if self._adds:
            changes['adds'] = list(self._adds)
        return changes

    # collections.GSet API, operates only on the immutable version
    def __contains__(self, element):
        return element in self.value

    def __iter__(self):
        return iter(self.value)

    def __len__(self):
        return len(self.value)

    # Sort of like collections.MutableSet API, without the additional
    # methods.
    def add(self, element):
        """
        Adds an element to the gset.

        .. note: You may add elements that already exist in the set.
           This may be used as an "assertion" that the element is a
           member.

        :param element: the element to add
        :type element: str
        """
        _check_element(element)
        self._adds.add(element)

    def _coerce_value(self, new_value):
        return frozenset(new_value)

    def _check_type(self, new_value):
        if not isinstance(new_value, collections.Iterable):
            return False
        for element in new_value:
            if not isinstance(element, string_types):
                return False
        return True


def _check_element(element):
    if not isinstance(element, string_types):
        raise TypeError("GSet elements can only be strings")


TYPES['gset'] = GSet
