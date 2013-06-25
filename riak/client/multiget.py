"""
Copyright 2013 Basho Technologies, Inc.

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

from collections import namedtuple
from Queue import Queue
from threading import Thread, Lock, Event
from multiprocessing import cpu_count

__all__ = ['multiget']


try:
    POOL_SIZE = cpu_count() * 2
except NotImplementedError:
    # Make an educated guess
    POOL_SIZE = 6


Task = namedtuple('Task', ['client', 'outq', 'bucket', 'key', 'options'])


class MultiGetPool(object):
    """
    Encapsulates a pool of fetcher threads. These threads can be used
    across many multi-get requests.
    """

    def __init__(self, size=POOL_SIZE):
        self._inq = Queue()
        self._size = size
        self._started = Event()
        self._stop = Event()
        self._lock = Lock()
        self._workers = []

    def enq(self, task):
        """
        Enqueues a fetch task to the pool of workers. This will raise
        a RuntimeError if the pool is stopped or in the process of
        stopping.

        :param task: the Task object
        :type task: Task
        """
        if not self._stop.is_set():
            self._inq.put(task)
        else:
            raise RuntimeError("Attempted to enqueue a fetch operation while "
                               "multi-get pool was shutdown!")

    def start(self):
        """
        Starts the worker threads if they are not already started.
        This method is thread-safe and will be called automatically
        when executing a MultiGet operation.
        """
        # Check whether we are already started, skip if we are.
        if not self._started.is_set():
            # If we are not started, try to capture the lock.
            if self._lock.acquire(False):
                # If we got the lock, go ahead and start the worker
                # threads, set the started flag, and release the lock.
                for i in range(self._size):
                    name = "riak.client.multiget-worker-{0}".format(i)
                    worker = Thread(target=self._fetcher, args=(name,),
                                    name=name)
                    worker.daemon = True
                    worker.start()
                    self._workers.append(worker)
                self._started.set()
                self._lock.release()
            else:
                # We didn't get the lock, so someone else is already
                # starting the worker threads. Wait until they have
                # signaled that the threads are started.
                self._started.wait()

    def stop(self):
        """
        Signals the worker threads to exit and waits on them.
        """
        self._stop.set()
        for worker in self._workers:
            worker.join()

    def stopped(self):
        """
        Detects whether this pool has been stopped.
        """
        return self._stop.is_set()

    def __del__(self):
        # Ensure that all work in the queue is processed before
        # shutting down.
        self.stop()

    def _fetcher(self, name):
        """
        The body of the multi-get worker.
        """
        while not self._should_quit():
            task = self._inq.get()
            try:
                obj = task.client.bucket(task.bucket).get(task.key,
                                                          **task.options)
                task.outq.put(obj)
            except KeyboardInterrupt:
                raise
            except StandardError as err:
                task.outq.put((task.bucket, task.key, err), )
            finally:
                self._inq.task_done()

    def _should_quit(self):
        """
        Worker threads should exit when the stop flag is set and the
        input queue is empty. Once the stop flag is set, new enqueues
        are disallowed, meaning that the workers can safely drain the
        queue before exiting.
        :rtype boolean
        """
        return self.stopped() and self._inq.empty()


RIAK_MULTIGET_POOL = MultiGetPool()


def multiget(client, keys, **options):
    """
    Executes a parallel-fetch across multiple threads. Returns a list
    containing RiakObject instances, or 3-tuples of bucket, key, and
    the exception raised.

    :rtype list
    """
    outq = Queue()

    RIAK_MULTIGET_POOL.start()
    for bucket, key in keys:
        task = Task(client=client, outq=outq, options=options,
                    bucket=bucket, key=key)
        RIAK_MULTIGET_POOL.enq(task)

    results = []
    for _ in range(len(keys)):
        if RIAK_MULTIGET_POOL.stopped():
            raise RuntimeError("Multi-get operation interrupted by pool "
                                   "stopping!")
        results.append(outq.get())
        outq.task_done()

    return results
