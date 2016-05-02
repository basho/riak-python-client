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


class RiakError(Exception):
    """
    Base class for exceptions generated in the Riak API.
    """
    def __init__(self, *args, **kwargs):
        super(RiakError, self).__init__(*args, **kwargs)
        if len(args) > 0:
            self.value = args[0]
        else:
            self.value = 'unknown'

    def __str__(self):
        return repr(self.value)


class ConflictError(RiakError):
    """
    Raised when an operation is attempted on a
    :class:`~riak.riak_object.RiakObject` that has more than one
    sibling.
    """
    def __init__(self, message='Object in conflict'):
        super(ConflictError, self).__init__(message)
