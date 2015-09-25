"""
Copyright 2012-2015 Basho Technologies, Inc.

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
from riak.transports.feature_detect import FeatureDetection

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest


class IncompleteTransport(FeatureDetection):
    pass


class DummyTransport(FeatureDetection):
    def __init__(self, version):
        self._version = version

    def _server_version(self):
        return self._version


class FeatureDetectionTest(unittest.TestCase):
    def test_implements_server_version(self):
        t = IncompleteTransport()

        with self.assertRaises(NotImplementedError):
            t.server_version

    def test_pre_10(self):
        t = DummyTransport("0.14.2")
        self.assertFalse(t.phaseless_mapred())
        self.assertFalse(t.pb_indexes())
        self.assertFalse(t.pb_search())
        self.assertFalse(t.pb_conditionals())
        self.assertFalse(t.quorum_controls())
        self.assertFalse(t.tombstone_vclocks())
        self.assertFalse(t.pb_head())
        self.assertFalse(t.pb_clear_bucket_props())
        self.assertFalse(t.pb_all_bucket_props())
        self.assertFalse(t.counters())
        self.assertFalse(t.stream_indexes())
        self.assertFalse(t.index_term_regex())
        self.assertFalse(t.bucket_types())
        self.assertFalse(t.datatypes())

    def test_10(self):
        t = DummyTransport("1.0.3")
        self.assertFalse(t.phaseless_mapred())
        self.assertFalse(t.pb_indexes())
        self.assertFalse(t.pb_search())
        self.assertTrue(t.pb_conditionals())
        self.assertTrue(t.quorum_controls())
        self.assertTrue(t.tombstone_vclocks())
        self.assertTrue(t.pb_head())
        self.assertFalse(t.pb_clear_bucket_props())
        self.assertFalse(t.pb_all_bucket_props())
        self.assertFalse(t.counters())
        self.assertFalse(t.stream_indexes())
        self.assertFalse(t.index_term_regex())
        self.assertFalse(t.bucket_types())
        self.assertFalse(t.datatypes())

    def test_11(self):
        t = DummyTransport("1.1.4")
        self.assertTrue(t.phaseless_mapred())
        self.assertFalse(t.pb_indexes())
        self.assertFalse(t.pb_search())
        self.assertTrue(t.pb_conditionals())
        self.assertTrue(t.quorum_controls())
        self.assertTrue(t.tombstone_vclocks())
        self.assertTrue(t.pb_head())
        self.assertFalse(t.pb_clear_bucket_props())
        self.assertFalse(t.pb_all_bucket_props())
        self.assertFalse(t.counters())
        self.assertFalse(t.stream_indexes())
        self.assertFalse(t.index_term_regex())
        self.assertFalse(t.bucket_types())
        self.assertFalse(t.datatypes())

    def test_12(self):
        t = DummyTransport("1.2.0")
        self.assertTrue(t.phaseless_mapred())
        self.assertTrue(t.pb_indexes())
        self.assertTrue(t.pb_search())
        self.assertTrue(t.pb_conditionals())
        self.assertTrue(t.quorum_controls())
        self.assertTrue(t.tombstone_vclocks())
        self.assertTrue(t.pb_head())
        self.assertFalse(t.pb_clear_bucket_props())
        self.assertFalse(t.pb_all_bucket_props())
        self.assertFalse(t.counters())
        self.assertFalse(t.stream_indexes())
        self.assertFalse(t.index_term_regex())
        self.assertFalse(t.bucket_types())
        self.assertFalse(t.datatypes())

    def test_12_loose(self):
        t = DummyTransport("1.2.1p3")
        self.assertTrue(t.phaseless_mapred())
        self.assertTrue(t.pb_indexes())
        self.assertTrue(t.pb_search())
        self.assertTrue(t.pb_conditionals())
        self.assertTrue(t.quorum_controls())
        self.assertTrue(t.tombstone_vclocks())
        self.assertTrue(t.pb_head())
        self.assertFalse(t.pb_clear_bucket_props())
        self.assertFalse(t.pb_all_bucket_props())
        self.assertFalse(t.counters())
        self.assertFalse(t.stream_indexes())
        self.assertFalse(t.index_term_regex())
        self.assertFalse(t.bucket_types())
        self.assertFalse(t.datatypes())

    def test_14(self):
        t = DummyTransport("1.4.0rc1")
        self.assertTrue(t.phaseless_mapred())
        self.assertTrue(t.pb_indexes())
        self.assertTrue(t.pb_search())
        self.assertTrue(t.pb_conditionals())
        self.assertTrue(t.quorum_controls())
        self.assertTrue(t.tombstone_vclocks())
        self.assertTrue(t.pb_head())
        self.assertTrue(t.pb_clear_bucket_props())
        self.assertTrue(t.pb_all_bucket_props())
        self.assertTrue(t.counters())
        self.assertTrue(t.stream_indexes())
        self.assertFalse(t.index_term_regex())
        self.assertFalse(t.bucket_types())
        self.assertFalse(t.datatypes())

    def test_144(self):
        t = DummyTransport("1.4.6")
        self.assertTrue(t.phaseless_mapred())
        self.assertTrue(t.pb_indexes())
        self.assertTrue(t.pb_search())
        self.assertTrue(t.pb_conditionals())
        self.assertTrue(t.quorum_controls())
        self.assertTrue(t.tombstone_vclocks())
        self.assertTrue(t.pb_head())
        self.assertTrue(t.pb_clear_bucket_props())
        self.assertTrue(t.pb_all_bucket_props())
        self.assertTrue(t.counters())
        self.assertTrue(t.stream_indexes())
        self.assertTrue(t.index_term_regex())
        self.assertFalse(t.bucket_types())
        self.assertFalse(t.datatypes())

    def test_20(self):
        t = DummyTransport("2.0.1")
        self.assertTrue(t.phaseless_mapred())
        self.assertTrue(t.pb_indexes())
        self.assertTrue(t.pb_search())
        self.assertTrue(t.pb_conditionals())
        self.assertTrue(t.quorum_controls())
        self.assertTrue(t.tombstone_vclocks())
        self.assertTrue(t.pb_head())
        self.assertTrue(t.pb_clear_bucket_props())
        self.assertTrue(t.pb_all_bucket_props())
        self.assertTrue(t.counters())
        self.assertTrue(t.stream_indexes())
        self.assertTrue(t.index_term_regex())
        self.assertTrue(t.bucket_types())
        self.assertTrue(t.datatypes())

if __name__ == '__main__':
    unittest.main()
