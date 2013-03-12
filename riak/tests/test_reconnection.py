import os
import re
import tempfile
import logging
import unittest
import time
import socket

import riak
from riak.test_server import TestServer, Atom

log = logging.getLogger(__name__)


class RiakServer(TestServer):
    APP_CONFIG_DEFAULTS = {
        "riak_api": {
            "pb_ip": "127.0.0.1",
            "pb_port": 0,
        },
        "riak_core": {
            "http": {'"127.0.0.1"': 0},
            "handoff_port": 0,
            "ring_creation_size": 8,
            "https": [],
        },
        "riak_kv": {
            "storage_backend": Atom("riak_kv_eleveldb_backend"),
            "mapred_2i_pipe": True,
            "pb_ip": Atom("undefined"),
            "pb_port": Atom("undefined"),
            "js_vm_count": 2,
            "js_max_vm_mem": 2,
            "js_thread_stack": 4,
            "riak_kv_stat": True,
            "map_cache_size": 0,
            "vnode_cache_entries": 0,
            "add_paths": [
            ],
            "http_url_encoding": Atom("on"),
        },
        "riak_search": {
            "enabled": True,
            "search_backend": Atom("riak_search_test_backend")
        },
        "riak_control": {
            "enabled": False
        },
        "eleveldb": {
            "data_root": None,
            "sync": False
        },
        "merge_index": {
            "buffer_rollover_size": 1048576,
            "data_root": None,
            "max_compact_segments": 20
        }
    }

    def __init__(self, config=None, start_timeout=120):
        if config is None:
            config = {}

        self.start_timeout = start_timeout

        riak_location = os.environ.get(
            'RIAK_BIN_DIR',
            os.path.expanduser("~/.riak/install/riak-0.14.2/bin"))

        tmpdir = tempfile.mkdtemp(prefix='riak-test-')

        super(RiakServer, self).__init__(tmpdir, riak_location, **config)

        self.setup_lager()

    def free_port(self):
        free_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        free_socket.bind(('127.0.0.1', 0))
        free_socket.listen(5)
        port = free_socket.getsockname()[1]
        free_socket.close()
        return port

    def setup_lager(self):
        logging_conf = '[{"%s/console.log", info, 10485760, "$D0", 5}]' % os.path.join(self.temp_dir, 'log')
        self.app_config["lager"]["handlers"] = {"lager_file_backend": Atom(logging_conf)}
        self.app_config["lager"]["error_logger_redirect"] = True

    def start(self):
        self.http_port = self.free_port()
        self.handoff_port = self.free_port()
        self.pb_port = self.free_port()

        self.app_config["riak_core"]["http"]['"127.0.0.1"'] = self.http_port
        self.app_config["riak_core"]["handoff_port"] = self.handoff_port
        self.app_config["riak_api"]["pb_port"] = self.pb_port
        self.app_config["eleveldb"]["data_root"] = os.path.join(self.temp_dir, "data", "leveldb")
        self.app_config["merge_index"]["data_root"] = os.path.join(self.temp_dir, "data", "merge_index")

        super(RiakServer, self).prepare()
        super(RiakServer, self).start()
        log.info("Server started")

    def restart(self):
        log.info("Stopping the server")
        self.stop()
        log.info("Restarting the server")
        super(RiakServer, self).start()
        log.info("Server restarted")

RIAK = RiakServer()

def setUpModule():
    RIAK.start()

def tearDownModule():
    RIAK.stop()
    RIAK.cleanup()

def _cleanup(conn, bucket_name):
    bucket = conn.bucket(bucket_name)
    for key in bucket.get_keys():
        obj = riak.RiakObject(conn, bucket, key)
        obj.delete()
    # wait until the deletion is propagated to every node
    while bucket.get_keys():
        time.sleep(0.01)

class BaseReconnection(object):
    def setUp(self):
        self.nodes = [dict(host='localhost', http_port=RIAK.http_port, pb_port=RIAK.pb_port)]
        self.bucket = "test-bucket"

    def tearDown(self):
        _cleanup(self.conn, self.bucket)

    def test(self):
        bucket = self.conn.bucket(self.bucket)

        obj = bucket.new("TEST1", "")
        obj.store()
        obj = bucket.new("TEST2", "")
        obj.store()
        RIAK.restart()
        obj = bucket.new("TEST3", "")
        obj.store()

        all_keys = bucket.get_keys()
        expected_keys = ['TEST1', 'TEST2', 'TEST3']
        self.assertItemsEqual(all_keys, expected_keys)

class TestHTTPReconnection(BaseReconnection, unittest.TestCase):
    def setUp(self):
        super(TestHTTPReconnection, self).setUp()

        self.conn = riak.RiakClient('http', nodes=self.nodes)

class TestPbcReconnection(BaseReconnection, unittest.TestCase):
    def setUp(self):
        super(TestPbcReconnection, self).setUp()

        self.conn = riak.RiakClient('pbc', nodes=self.nodes)

if __name__ == "__main__":
    unittest.main()
