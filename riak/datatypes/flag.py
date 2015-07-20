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


class Flag(Datatype):
    """
    A convergent datatype that represents a boolean value that can be
    enabled or disabled, and may only be embedded in :class:`Map`
    instances.
    """

    type_name = 'flag'
    _type_error_msg = "Flags can only be booleans"

    def _post_init(self):
        self._op = None

    def _default_value(self):
        return False

    @Datatype.modified.getter
    def modified(self):
        """
        Whether this flag has staged toggles.
        """
        return self._op is not None

    def enable(self):
        """
        Turns the flag on, effectively setting its value to ``True``.
        """
        self._op = 'enable'

    def disable(self):
        """
        Turns the flag off, effectively setting its value to ``False``.
        """
        self._require_context()
        self._op = 'disable'

    def to_op(self):
        """
        Extracts the mutation operation from the flag.

        :rtype: bool, None
        """
        return self._op

    def _check_type(self, new_value):
        return isinstance(new_value, bool)


TYPES['flag'] = Flag
