# -*- coding: utf-8 -*-
import unittest

from riak.transports.feature_detect import FeatureDetection


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
        self.assertFalse(t.preflists())
        self.assertFalse(t.write_once())

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
        self.assertFalse(t.preflists())
        self.assertFalse(t.write_once())

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
        self.assertFalse(t.preflists())
        self.assertFalse(t.write_once())

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
        self.assertFalse(t.preflists())
        self.assertFalse(t.write_once())

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
        self.assertFalse(t.preflists())
        self.assertFalse(t.write_once())

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
        self.assertFalse(t.preflists())
        self.assertFalse(t.write_once())

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
        self.assertFalse(t.preflists())
        self.assertFalse(t.write_once())

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
        self.assertFalse(t.preflists())
        self.assertFalse(t.write_once())

    def test_21(self):
        t = DummyTransport("2.1.0")
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
        self.assertTrue(t.preflists())
        self.assertTrue(t.write_once())


if __name__ == '__main__':
    unittest.main()
