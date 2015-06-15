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

from collections import Sized
from riak.datatypes.datatype import Datatype
from six import string_types
from riak.datatypes import TYPES


class Register(Sized, Datatype):
    """
    A convergent datatype that represents an opaque string that is set
    with last-write-wins semantics, and may only be embedded in
    :class:`~riak.datatypes.Map` instances.
    """

    type_name = 'register'
    _type_error_msg = "Registers can only be strings"

    def _post_init(self):
        self._new_value = None

    def _default_value(self):
        return ""

    @Datatype.value.getter
    def value(self):
        """
        Returns a copy of the original value of the register.

        :rtype: str
        """
        return self._value[:]

    @Datatype.modified.getter
    def modified(self):
        """
        Whether this register has staged assignment.
        """
        return self._new_value is not None

    def to_op(self):
        """
        Extracts the mutation operation from the register.

        :rtype: str, None
        """
        if self._new_value is not None:
            return ('assign', self._new_value)

    def assign(self, new_value):
        """
        Assigns a new value to the register.

        :param new_value: the new value for the register
        :type new_value: str
        """
        self._raise_if_badtype(new_value)
        self._new_value = new_value

    def __len__(self):
        return len(self.value)

    def _check_type(self, new_value):
        return isinstance(new_value, string_types)


TYPES['register'] = Register
