from __future__ import print_function
import os.path
import threading
import string
import re
import random
import shutil
import socket
import time
import stat
from subprocess import Popen, PIPE
from riak.util import deep_merge
from six import string_types

try:
    bytes
except NameError:
    bytes = str


class Atom(object):
    def __init__(self, s):
        self.str = s

    def __str__(self):
        return str(self.str)

    def __repr__(self):
        return repr(self.str)

    def __eq__(self, other):
        return self.str == other

    def __lt__(self, other):
        return self.str < other


def erlang_config(hash, depth=1):
    def printable(item):
        k, v = item
        if isinstance(v, string_types):
            p = '"%s"' % v
        elif isinstance(v, dict):
            p = erlang_config(v, depth + 1)
        elif isinstance(v, bool):
            p = ("%s" % v).lower()
        else:
            p = "%s" % v

        return "{%s, %s}" % (k, p)

    padding = '    ' * depth
    parent_padding = '    ' * (depth - 1)
    values = (",\n%s" % padding).join(map(printable, hash.items()))
    return "[\n%s%s\n%s]" % (padding, values, parent_padding)


class TestServer(object):
    VM_ARGS_DEFAULTS = {
        "-name": "riaktest%d@127.0.0.1" % random.randint(0, 100000),
        "-setcookie": "%d_%d" % (random.randint(0, 100000),
                                 random.randint(0, 100000)),
        "+K": "true",
        "+A": 64,
        "-smp": "enable",
        "-env ERL_MAX_PORTS": 4096,
        "-env ERL_FULLSWEEP_AFTER": 10,
        "-pa": os.path.abspath(os.path.join(os.path.dirname(__file__),
                                            "erl_src"))
    }

    APP_CONFIG_DEFAULTS = {
        "riak_core": {
            "web_ip": "127.0.0.1",
            "web_port": 9000,
            "handoff_port": 9001,
            "ring_creation_size": 64
        },
        "riak_kv": {
            "storage_backend": Atom("riak_kv_test_backend"),
            "pb_ip": "127.0.0.1",
            "pb_port": 9002,
            "js_vm_count": 8,
            "js_max_vm_mem": 8,
            "js_thread_stack": 16,
            "riak_kv_stat": True,
            "map_cache_size": 0,
            "vnode_cache_entries": 0,
            "test": True,
            "memory_backend": {
                "test": True,
            },
        },
        "riak_search": {
            "enabled": True,
            "search_backend": Atom("riak_search_test_backend")
        },
    }

    DEFAULT_BASE_DIR = "RUNNER_BASE_DIR=${RUNNER_SCRIPT_DIR%/*}"

    _temp_bin = None
    _temp_etc = None
    _temp_log = None
    _temp_pipe = None

    def __init__(self, tmp_dir="/tmp/riak/test_server",
                 bin_dir=os.path.expanduser("~/.riak/install/riak-0.14.2/bin"),
                 vm_args=None, **options):
        self._lock = threading.Lock()
        self.temp_dir = tmp_dir
        self.bin_dir = bin_dir
        self._prepared = False
        self._started = False
        self.vm_args = self.VM_ARGS_DEFAULTS.copy()
        if vm_args is not None:
            self.vm_args = deep_merge(self.vm_args, vm_args)

        self.app_config = self.APP_CONFIG_DEFAULTS.copy()
        for key, value in options.items():
            if key in self.app_config:
                self.app_config[key] = deep_merge(self.app_config[key], value)
        ring_dir = os.path.join(self.temp_dir, "data", "ring")
        crash_log = os.path.join(self.temp_dir, "log", "crash.log")
        self.app_config["riak_core"]["ring_state_dir"] = ring_dir
        self.app_config["riak_core"]["platform_data_dir"] = self.temp_dir
        self.app_config["lager"] = {"crash_log": crash_log}

    def prepare(self):
        if not self._prepared:
            self.touch_ssl_distribution_args()
            self.create_temp_directories()
            self._riak_script = os.path.join(self._temp_bin, "riak")
            self.write_riak_script()
            self.write_vm_args()
            self.write_app_config()
            self._prepared = True

    def create_temp_directories(self):
        directories = ["bin", "etc", "log", "data", "pipe"]
        for directory in directories:
            dir = os.path.normpath(os.path.join(self.temp_dir, directory))
            if not os.path.exists(dir):
                os.makedirs(dir)
            setattr(self, "_temp_%s" % directory, dir)

    def start(self):
        if self._prepared and not self._started:
            with self._lock:
                self._server = Popen([self._riak_script, "console"],
                                     stdin=PIPE, stdout=PIPE, stderr=PIPE)
                self._server.stdin.write("\n")
                self._server.stdin.flush()
                self.wait_for_erlang_prompt()
                self._started = True

    def stop(self):
        if self._started:
            with self._lock:
                self._server.stdin.write("init:stop().\n")
                self._server.stdin.flush()
                self._server.wait()
                self._started = False

    def cleanup(self):
        if self._started:
            self.stop()

        shutil.rmtree(self.temp_dir, True)
        self._prepared = False

    def recycle(self):
        if self._started:
            with self._lock:
                stdin = self._server.stdin
                if self._kv_backend() == "riak_kv_test_backend":
                    stdin.write("riak_kv_test_backend:reset().\n")
                    stdin.flush()
                    self.wait_for_erlang_prompt()

                    if self.app_config["riak_search"]["enabled"]:
                        stdin.write("riak_search_test_backend:reset().\n")
                        stdin.flush()
                        self.wait_for_erlang_prompt()
                else:
                    stdin.write("init:restart().\n")
                    stdin.flush()
                    self.wait_for_erlang_prompt()
                    self.wait_for_startup()

    def wait_for_startup(self):
        listening = False
        while not listening:
            try:
                socket.create_connection((self._http_ip(), self._http_port()),
                                         1.0)
            except IOError:
                pass
            else:
                listening = True

    def wait_for_erlang_prompt(self):
        prompted = False
        buffer = ""
        while not prompted:
            line = self._server.stdout.readline()
            if len(line) > 0:
                buffer += line
            if re.search(r"\(%s\)\d+>" % self.vm_args["-name"], buffer):
                prompted = True
            if re.search(r'"Kernel pid terminated".*\n', buffer):
                raise Exception("Riak test server failed to start.")

    def write_riak_script(self):
        with open(self._riak_script, "wb") as temp_bin_file:
            with open(os.path.join(self.bin_dir, "riak"), "r") as riak_file:
                for line in riak_file.readlines():
                    line = re.sub("(RUNNER_SCRIPT_DIR=)(.*)", r'\1%s' %
                                  self._temp_bin,
                                  line)
                    line = re.sub("(RUNNER_ETC_DIR=)(.*)", r'\1%s' %
                                  self._temp_etc, line)
                    line = re.sub("(RUNNER_USER=)(.*)", r'\1', line)
                    line = re.sub("(RUNNER_LOG_DIR=)(.*)", r'\1%s' %
                                  self._temp_log, line)
                    line = re.sub("(PIPE_DIR=)(.*)", r'\1%s' %
                                  self._temp_pipe, line)
                    line = re.sub("(PLATFORM_DATA_DIR=)(.*)", r'\1%s' %
                                  self.temp_dir, line)

                    if (string.strip(line) == self.DEFAULT_BASE_DIR):
                        line = ("RUNNER_BASE_DIR=%s\n" %
                                os.path.normpath(os.path.join(self.bin_dir,
                                                              "..")))

                    temp_bin_file.write(line)

                os.fchmod(temp_bin_file.fileno(),
                          stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP |
                          stat.S_IROTH | stat.S_IXOTH)

    def write_vm_args(self):
        with open(self._vm_args_path(), 'wb') as vm_args:
            for arg, value in self.vm_args.items():
                vm_args.write("%s %s\n" % (arg, value))

    def write_app_config(self):
        with open(self._app_config_path(), "wb") as app_config:
            app_config.write(erlang_config(self.app_config))
            app_config.write(".")

    def touch_ssl_distribution_args(self):
        # To make sure that the ssl_distribution.args file is present,
        # the control script in the source node has to have been run at
        # least once. Running the `chkconfig` command is innocuous
        # enough to accomplish this without other side-effects.
        script = os.path.join(self.bin_dir, "riak")
        Popen([script, "chkconfig"],
              stdin=PIPE, stdout=PIPE, stderr=PIPE).communicate()

    def _kv_backend(self):
        return self.app_config["riak_kv"]["storage_backend"]

    def _http_ip(self):
        return self.app_config["riak_core"]["web_ip"]

    def _http_port(self):
        return self.app_config["riak_core"]["web_port"]

    def _app_config_path(self):
        return os.path.join(self._temp_etc, "app.config")

    def _vm_args_path(self):
        return os.path.join(self._temp_etc, "vm.args")


if __name__ == "__main__":
    server = TestServer()
    server.prepare()
    server.start()
    print("Started...")
    time.sleep(20)
    print("Recycling...")
    server.recycle()
    time.sleep(20)
    server.stop()
    server.cleanup()
