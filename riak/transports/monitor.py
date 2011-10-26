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
