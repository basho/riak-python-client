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

from riak import RiakError


class ContextRequired(RiakError):
    """
    This exception is raised when removals of map fields and set
    entries are attempted and the datatype hasn't been initialized
    with a context.
    """

    _default_message = ("A context is required for remove operations, "
                        "fetch the datatype first")

    def __init__(self, message=None):
        super(ContextRequired, self).__init__(message or
                                              self._default_message)
