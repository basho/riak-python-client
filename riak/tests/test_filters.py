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

import platform
from riak.mapreduce import RiakKeyFilter
from riak import key_filter

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest


class FilterTests(unittest.TestCase):
    def test_simple(self):
        f1 = RiakKeyFilter("tokenize", "-", 1)
        self.assertEqual(f1._filters, [["tokenize", "-", 1]])

    def test_add(self):
        f1 = RiakKeyFilter("tokenize", "-", 1)
        f2 = RiakKeyFilter("eq", "2005")
        f3 = f1 + f2
        self.assertEqual(list(f3), [["tokenize", "-", 1], ["eq", "2005"]])

    def test_and(self):
        f1 = RiakKeyFilter("starts_with", "2005-")
        f2 = RiakKeyFilter("ends_with", "-01")
        f3 = f1 & f2
        self.assertEqual(list(f3),
                         [["and",
                           [["starts_with", "2005-"]],
                           [["ends_with", "-01"]]]])

    def test_multi_and(self):
        f1 = RiakKeyFilter("starts_with", "2005-")
        f2 = RiakKeyFilter("ends_with", "-01")
        f3 = RiakKeyFilter("matches", "-11-")
        f4 = f1 & f2 & f3
        self.assertEqual(list(f4), [["and",
                                     [["starts_with", "2005-"]],
                                     [["ends_with", "-01"]],
                                     [["matches", "-11-"]],
                                     ]])

    def test_or(self):
        f1 = RiakKeyFilter("starts_with", "2005-")
        f2 = RiakKeyFilter("ends_with", "-01")
        f3 = f1 | f2
        self.assertEqual(list(f3), [["or", [["starts_with", "2005-"]],
                                     [["ends_with", "-01"]]]])

    def test_multi_or(self):
        f1 = RiakKeyFilter("starts_with", "2005-")
        f2 = RiakKeyFilter("ends_with", "-01")
        f3 = RiakKeyFilter("matches", "-11-")
        f4 = f1 | f2 | f3
        self.assertEqual(list(f4), [["or",
                                     [["starts_with", "2005-"]],
                                     [["ends_with", "-01"]],
                                     [["matches", "-11-"]],
                                     ]])

    def test_chaining(self):
        f1 = key_filter.tokenize("-", 1).eq("2005")
        f2 = key_filter.tokenize("-", 2).eq("05")
        f3 = f1 & f2
        self.assertEqual(list(f3), [["and",
                                     [["tokenize", "-", 1], ["eq", "2005"]],
                                     [["tokenize", "-", 2], ["eq", "05"]]
                                     ]])
