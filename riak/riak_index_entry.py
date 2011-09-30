"""
Copyright 2010 Rusty Klophaus <rusty@basho.com>
Copyright 2010 Justin Sheehy <justin@basho.com>
Copyright 2009 Jay Baird <jay@mochimedia.com>

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

class RiakIndexEntry:
    def __init__(self, field, value):
        self._field = field
        self._value = str(value)

    def get_field(self):
        return self._field

    def get_value(self):
        return self._value

    def __str__(self):
        return "RiakIndexEntry(field = '%s', value='%s')" % (self._field, self._value)

    def __eq__(self, other):
        if not isinstance(other, RiakIndexEntry):
            return False

        return \
            self.get_field() == other.get_field() and \
            self.get_value() == other.get_value()

    def __cmp__(self, other):
        if other == None:
            raise TypeError("RiakIndexEntry cannot be compared to None")

        if not isinstance(other, RiakIndexEntry):
            raise TypeError("RiakIndexEntry cannot be compared to %s" % other.__class__.__name__)

        if self.get_field() < other.get_field():
            return -1

        if self.get_field() > other.get_field():
            return 1

        if self.get_value() < other.get_value():
            return -1

        if self.get_value() > other.get_value():
            return 1

        return 0
