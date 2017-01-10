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

import unittest


class MiscTests(unittest.TestCase):
    def test_timeout_validation(self):
        from riak.client.operations import _validate_timeout
        # valid cases
        try:
            _validate_timeout(None)
            _validate_timeout(None, infinity_ok=True)
            _validate_timeout('infinity', infinity_ok=True)
            _validate_timeout(1234)
            _validate_timeout(1234567898765432123456789)
        except ValueError:
            self.fail('_validate_timeout() unexpectedly raised ValueError')
        # invalid cases
        with self.assertRaises(ValueError):
            _validate_timeout('infinity')
        with self.assertRaises(ValueError):
            _validate_timeout('infinity-foo')
        with self.assertRaises(ValueError):
            _validate_timeout('foobarbaz')
        with self.assertRaises(ValueError):
            _validate_timeout('1234')
        with self.assertRaises(ValueError):
            _validate_timeout(0)
        with self.assertRaises(ValueError):
            _validate_timeout(12.34)
