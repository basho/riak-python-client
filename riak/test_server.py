import os.path
import threading
import string
import re
import random
import shutil
from subprocess import Popen, PIPE

def erlang_config(hash, depth=1):
    def printable(item):
        k, v = item
        if isinstance(v, str):
            p = '"%s"' % v
        elif isinstance(v, dict):
            p = erlang_config(v, depth + 1)
        elif isinstance(v, bool):
            p = ("%s" % v).lower()
        else:
            p = "%s" % v

        return "{%s, %s}" % (k, p)

    padding = '    ' * depth
    parent_padding = '    ' * (depth-1)
    values = (",\n%s" % padding).join(map(printable, hash.items()))
    return "[\n%s%s\n%s]" % (padding, values, parent_padding)


class TestServer:
    VM_ARGS_DEFAULTS = {
        "-name": "riaktest%d@127.0.0.1" % random.randint(0, 100000),
        "-setcookie": "%d_%d" % (random.randint(0, 100000), random.randint(0, 100000)),
        "+K": "true",
        "+A": 64,
        "-smp": "enable",
        "-env ERL_MAX_PORTS": 4096,
        "-env ERL_FULLSWEEP_AFTER": 10,
        "-pa": os.path.abspath(os.path.join(os.path.dirname(__file__), "../erl_src"))
    }

    APP_CONFIG_DEFAULTS = {
      "riak_core": {
          "web_ip": "127.0.0.1",
          "web_port": 9000,
          "handoff_port": 9001,
          "ring_creation_size": 64
      },
      "riak_kv": {
          "storage_backend": bytearray("riak_kv_test_backend"),
          "pb_ip": "127.0.0.1",
          "pb_port": 9002,
          "js_vm_count": 8,
          "js_max_vm_mem": 8,
          "js_thread_stack": 16,
          "riak_kv_stat": True,
          "map_cache_size": 0,
          "vnode_cache_entries": 0
      },
      "riak_search": {
          "enabled": True,
          "search_backend": bytearray("riak_search_test_backend")
      },
      "luwak": {
          "enabled": True
      }
    }

    def __init__(self, tmp_dir="/tmp/riak/test_server",
                 bin_dir=os.path.expanduser("~/.riak/install/riak-0.14.2/bin")):
        self._lock = threading.Lock()
        self.temp_dir = "/tmp/riak/test_server"
        self.bin_dir = bin_dir
        self._prepared = False
        self._started = False
        self.vm_args = self.__class__.VM_ARGS_DEFAULTS
        self.app_config = self.__class__.APP_CONFIG_DEFAULTS
        self.app_config["riak_core"]["ring_state_dir"] = os.path.join(self.temp_dir, "data", "ring")

    def prepare(self):
        if not self._prepared:
            self.create_temp_directories()
            self._riak_script = os.path.join(self._temp_bin, "riak")
            self.__write_riak_script()
            self.__write_vm_args()
            self.__write_app_config()
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
                self._server = Popen([self._riak_script, "console"], stdin=PIPE, stdout=PIPE, stderr=PIPE)
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

    def wait_for_erlang_prompt(self):
        prompted = False
        buffer = ""
        while not prompted:
            line = self._server.stdout.read(1)
            if len(line) > 0:
                buffer += line
            if re.search(r"\(%s\)\d+>" % self.vm_args["-name"], buffer):
                print("Started...")
                prompted = True

    def __write_riak_script(self):
        with open(self._riak_script, "wb") as temp_bin_file, open(os.path.join(self.bin_dir, "riak"), "r") as riak_file:
                
            for line in riak_file.readlines():
                line = re.sub("(RUNNER_SCRIPT_DIR=)(.*)", r'\1%s' % self._temp_bin, line)
                line = re.sub("(RUNNER_ETC_DIR=)(.*)", r'\1%s' % self._temp_etc, line)
                line = re.sub("(RUNNER_USER=)(.*)", r'\1', line)
                line = re.sub("(RUNNER_LOG_DIR=)(.*)", r'\1%s' % self._temp_log, line)
                line = re.sub("(PIPE_DIR=)(.*)", r'\1%s' % self._temp_pipe, line)

                if string.strip(line) == "RUNNER_BASE_DIR=${RUNNER_SCRIPT_DIR%/*}":
                    line = "RUNNER_BASE_DIR=%s\n" % os.path.normpath(os.path.join(self.bin_dir, ".."))

                temp_bin_file.write(line)

            os.fchmod(temp_bin_file.fileno(), 0755)

    def __write_vm_args(self):
        with open(os.path.join(self._temp_etc, "vm.args"), 'wb') as vm_args:
            for arg, value in self.vm_args.items():
                vm_args.write("%s %s\n" % (arg, value))

    def __write_app_config(self):
        with open(os.path.join(self._temp_etc, "app.config"), "wb") as app_config:
            app_config.write(erlang_config(self.app_config))
            app_config.write(".")


if __name__ == "__main__":
    server = TestServer()
    server.prepare()
    server.start()
    server.stop()
    server.cleanup()
