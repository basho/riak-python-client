"""
Copyright 2014 Basho Technologies, Inc.

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
from six import PY2


class Comparison(object):
    '''
    Provide a cross-version object comparison operator
    since its name changed between Python 2.x and Python 3.x
    '''

    def assert_items_equal(self, first, second, msg=None):
        if PY2:
            self.assertItemsEqual(first, second, msg)
        else:
            self.assertCountEqual(first, second, msg)

    def assert_raises_regex(self, exception, regexp, msg=None):
        if PY2:
            return self.assertRaisesRegexp(exception, regexp, msg)
        else:
            return self.assertRaisesRegex(exception, regexp, msg)
