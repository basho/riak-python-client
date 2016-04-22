import sys
import unittest

from riak.test_server import TestServer


@unittest.skipIf(sys.platform == 'win32', 'Windows is not supported')
class TestServerTestCase(unittest.TestCase):
    def setUp(self):
        self.test_server = TestServer()

    def tearDown(self):
        pass

    def test_options_defaults(self):
        self.assertEqual(
            self.test_server.app_config["riak_core"]["handoff_port"], 9001)
        self.assertEqual(
            self.test_server.app_config["riak_kv"]["pb_ip"], "127.0.0.1")

    def test_merge_riak_core_options(self):
        self.test_server = TestServer(riak_core={"handoff_port": 10000})
        self.assertEqual(
            self.test_server.app_config["riak_core"]["handoff_port"], 10000)

    def test_merge_riak_search_options(self):
        self.test_server = TestServer(
            riak_search={"search_backend": "riak_search_backend"})
        self.assertEqual(
            self.test_server.app_config["riak_search"]["search_backend"],
            "riak_search_backend")

    def test_merge_riak_kv_options(self):
        self.test_server = TestServer(riak_kv={"pb_ip": "192.168.2.1"})
        self.assertEqual(self.test_server.app_config["riak_kv"]["pb_ip"],
                         "192.168.2.1")

    def test_merge_vmargs(self):
        self.test_server = TestServer(vm_args={"-P": 65000})
        self.assertEqual(self.test_server.vm_args["-P"], 65000)

    def test_set_ring_state_dir(self):
        self.assertEqual(
            self.test_server.app_config["riak_core"]["ring_state_dir"],
            "/tmp/riak/test_server/data/ring")

    def test_set_default_tmp_dir(self):
        self.assertEqual(self.test_server.temp_dir, "/tmp/riak/test_server")

    def test_set_non_default_tmp_dir(self):
        tmp_dir = '/not/the/default/dir'
        server = TestServer(tmp_dir=tmp_dir)
        self.assertEqual(server.temp_dir, tmp_dir)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestServerTestCase())
    return suite
