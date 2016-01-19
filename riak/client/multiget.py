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

from __future__ import print_function
from collections import namedtuple
from threading import Thread, Lock, Event
from multiprocessing import cpu_count
from six import PY2
if PY2:
    from Queue import Queue
else:
    from queue import Queue

__all__ = ['multiget', 'MultiGetPool']


try:
    #: The default size of the worker pool, either based on the number
    #: of CPUS or defaulting to 6
    POOL_SIZE = cpu_count()
except NotImplementedError:
    # Make an educated guess
    POOL_SIZE = 6

#: A :class:`namedtuple` for tasks that are fed to workers in the
#: multiget pool.
Task = namedtuple('Task', ['client', 'outq', 'bucket_type', 'bucket', 'key',
                           'options'])


class MultiGetPool(object):
    """
    Encapsulates a pool of fetcher threads. These threads can be used
    across many multi-get requests.
    """

    def __init__(self, size=POOL_SIZE):
        """
        :param size: the desired size of the worker pool
        :type size: int
        """

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
                    worker = Thread(target=self._fetcher, name=name)
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

    def _fetcher(self):
        """
        The body of the multi-get worker. Loops until
        :meth:`_should_quit` returns ``True``, taking tasks off the
        input queue, fetching the object, and putting them on the
        output queue.
        """
        while not self._should_quit():
            task = self._inq.get()
            try:
                btype = task.client.bucket_type(task.bucket_type)
                obj = btype.bucket(task.bucket).get(task.key, **task.options)
                task.outq.put(obj)
            except KeyboardInterrupt:
                raise
            except Exception as err:
                task.outq.put((task.bucket_type, task.bucket, task.key, err), )
            finally:
                self._inq.task_done()

    def _should_quit(self):
        """
        Worker threads should exit when the stop flag is set and the
        input queue is empty. Once the stop flag is set, new enqueues
        are disallowed, meaning that the workers can safely drain the
        queue before exiting.

        :rtype: bool
        """
        return self.stopped() and self._inq.empty()


#: The default pool is automatically created and stored in this constant.
RIAK_MULTIGET_POOL = MultiGetPool()


def multiget(client, keys, **options):
    """Executes a parallel-fetch across multiple threads. Returns a list
    containing :class:`~riak.riak_object.RiakObject` or
    :class:`~riak.datatypes.Datatype` instances, or 4-tuples of
    bucket-type, bucket, key, and the exception raised.

    If a ``pool`` option is included, the request will use the given worker
    pool and not the default :data:`RIAK_MULTIGET_POOL`. This option will
    be passed by the client if the ``multiget_pool_size`` option was set on
    client initialization.

    :param client: the client to use
    :type client: :class:`~riak.client.RiakClient`
    :param keys: the keys to fetch in parallel
    :type keys: list of three-tuples -- bucket_type/bucket/key
    :param options: request options to
        :meth:`RiakBucket.get <riak.bucket.RiakBucket.get>`
    :type options: dict
    :rtype: list

    """
    outq = Queue()

    if 'pool' in options:
        pool = options['pool']
        del options['pool']
    else:
        pool = RIAK_MULTIGET_POOL

    pool.start()
    for bucket_type, bucket, key in keys:
        task = Task(client, outq, bucket_type, bucket, key, options)
        pool.enq(task)

    results = []
    for _ in range(len(keys)):
        if pool.stopped():
            raise RuntimeError("Multi-get operation interrupted by pool "
                               "stopping!")
        results.append(outq.get())
        outq.task_done()

    return results

if __name__ == '__main__':
    # Run a benchmark!
    from riak import RiakClient
    import riak.benchmark as benchmark
    client = RiakClient(protocol='pbc')
    bkeys = [('default', 'multiget', str(key)) for key in range(10000)]

    data = None
    with open(__file__) as f:
        data = f.read()

    print("Benchmarking multiget:")
    print("      CPUs: {0}".format(cpu_count()))
    print("   Threads: {0}".format(POOL_SIZE))
    print("      Keys: {0}".format(len(bkeys)))
    print()

    with benchmark.measure() as b:
        with b.report('populate'):
            for _, bucket, key in bkeys:
                client.bucket(bucket).new(key, encoded_data=data,
                                          content_type='text/plain'
                                          ).store()
    for b in benchmark.measure_with_rehearsal():
        client.protocol = 'http'
        with b.report('http seq'):
            for _, bucket, key in bkeys:
                client.bucket(bucket).get(key)

        with b.report('http multi'):
            multiget(client, bkeys)

        client.protocol = 'pbc'
        with b.report('pbc seq'):
            for _, bucket, key in bkeys:
                client.bucket(bucket).get(key)

        with b.report('pbc multi'):
            multiget(client, bkeys)
