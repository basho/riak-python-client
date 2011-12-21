"""
Copyright 2011 Greg Stein <gstein@gmail.com>

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

import threading
import time


class Monitor(object):

    def __init__(self, cm, transport):
        self._cm = cm
        self._transport = transport

        self._stop_loop = False
        self._thread = threading.Thread(target=self._run)

        self._periodic = 0.050  # 50 msec

    def start(self):
        self._thread.start()

    def terminate(self):
        self._stop_loop = True
        self._thread.join()

    def _run(self):
        while not self._stop_loop:
            ### look for changes in the ring servers

            ### see if some offline servers came back

            time.sleep(self._periodic)
