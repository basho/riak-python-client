from __future__ import print_function
from collections import namedtuple
from threading import Thread, Lock, Event
from multiprocessing import cpu_count
from six import PY2

from riak.riak_object import RiakObject
from riak.ts_object import TsObject

if PY2:
    from Queue import Queue, Empty
else:
    from queue import Queue, Empty

__all__ = ['multiget', 'multiput', 'MultiGetPool', 'MultiPutPool']


try:
    #: The default size of the worker pool, either based on the number
    #: of CPUS or defaulting to 6
    POOL_SIZE = cpu_count()
except NotImplementedError:
    # Make an educated guess
    POOL_SIZE = 6

#: A :class:`namedtuple` for tasks that are fed to workers in the
#: multi get pool.
Task = namedtuple('Task',
                  ['client', 'outq', 'bucket_type', 'bucket', 'key',
                   'object', 'options'])


#: A :class:`namedtuple` for tasks that are fed to workers in the
#: multi put pool.
PutTask = namedtuple('PutTask',
                     ['client', 'outq', 'object', 'options'])


class MultiPool(object):
    """
    Encapsulates a pool of threads. These threads can be used
    across many multi requests.
    """

    def __init__(self, size=POOL_SIZE, name='unknown'):
        """
        :param size: the desired size of the worker pool
        :type size: int
        """

        self._inq = Queue()
        self._size = size
        self._name = name
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
        :type task: Task or PutTask
        """
        if not self._stop.is_set():
            self._inq.put(task)
        else:
            raise RuntimeError("Attempted to enqueue an operation while "
                               "multi pool was shutdown!")

    def start(self):
        """
        Starts the worker threads if they are not already started.
        This method is thread-safe and will be called automatically
        when executing an operation.
        """
        # Check whether we are already started, skip if we are.
        if not self._started.is_set():
            # If we are not started, try to capture the lock.
            if self._lock.acquire(False):
                # If we got the lock, go ahead and start the worker
                # threads, set the started flag, and release the lock.
                for i in range(self._size):
                    name = "riak.client.multi-worker-{0}-{1}".format(
                            self._name, i)
                    worker = Thread(target=self._worker_method, name=name)
                    worker.daemon = False
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
        if not self.stopped():
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

    def _worker_method(self):
        raise NotImplementedError

    def _should_quit(self):
        """
        Worker threads should exit when the stop flag is set and the
        input queue is empty. Once the stop flag is set, new enqueues
        are disallowed, meaning that the workers can safely drain the
        queue before exiting.

        :rtype: bool
        """
        return self.stopped() and self._inq.empty()


class MultiGetPool(MultiPool):
    def __init__(self, size=POOL_SIZE):
        super(MultiGetPool, self).__init__(size=size, name='get')

    def _worker_method(self):
        """
        The body of the multi-get worker. Loops until
        :meth:`_should_quit` returns ``True``, taking tasks off the
        input queue, fetching the object, and putting them on the
        output queue.
        """
        while not self._should_quit():
            try:
                task = self._inq.get(block=True, timeout=0.25)
            except TypeError:
                if self._should_quit():
                    break
                else:
                    raise
            except Empty:
                continue

            try:
                btype = task.client.bucket_type(task.bucket_type)
                obj = btype.bucket(task.bucket).get(task.key, **task.options)
                task.outq.put(obj)
            except KeyboardInterrupt:
                raise
            except Exception as err:
                errdata = (task.bucket_type, task.bucket, task.key, err)
                task.outq.put(errdata)
            finally:
                self._inq.task_done()


class MultiPutPool(MultiPool):
    def __init__(self, size=POOL_SIZE):
        super(MultiPutPool, self).__init__(size=size, name='put')

    def _worker_method(self):
        """
        The body of the multi-put worker. Loops until
        :meth:`_should_quit` returns ``True``, taking tasks off the
        input queue, storing the object, and putting the result on
        the output queue.
        """
        while not self._should_quit():
            try:
                task = self._inq.get(block=True, timeout=0.25)
            except TypeError:
                if self._should_quit():
                    break
                else:
                    raise
            except Empty:
                continue

            try:
                obj = task.object
                if isinstance(obj, RiakObject):
                    rv = task.client.put(obj, **task.options)
                elif isinstance(obj, TsObject):
                    rv = task.client.ts_put(obj, **task.options)
                else:
                    raise ValueError('unknown obj type: %s'.format(type(obj)))
                task.outq.put(rv)
            except KeyboardInterrupt:
                raise
            except Exception as err:
                errdata = (task.object, err)
                task.outq.put(errdata)
            finally:
                self._inq.task_done()


def multiget(client, keys, **options):
    """Executes a parallel-fetch across multiple threads. Returns a list
    containing :class:`~riak.riak_object.RiakObject` or
    :class:`~riak.datatypes.Datatype` instances, or 4-tuples of
    bucket-type, bucket, key, and the exception raised.

    If a ``pool`` option is included, the request will use the given worker
    pool and not a transient :class:`~riak.client.multi.MultiGetPool`. This
    option will be passed by the client if the ``multiget_pool_size``
    option was set on client initialization.

    :param client: the client to use
    :type client: :class:`~riak.client.RiakClient`
    :param keys: the keys to fetch in parallel
    :type keys: list of three-tuples -- bucket_type/bucket/key
    :param options: request options to
        :meth:`RiakBucket.get <riak.bucket.RiakBucket.get>`
    :type options: dict
    :rtype: list

    """
    transient_pool = False
    outq = Queue()

    if 'pool' in options:
        pool = options['pool']
        del options['pool']
    else:
        pool = MultiGetPool()
        transient_pool = True

    try:
        pool.start()
        for bucket_type, bucket, key in keys:
            task = Task(client, outq, bucket_type, bucket, key, None, options)
            pool.enq(task)

        results = []
        for _ in range(len(keys)):
            if pool.stopped():
                raise RuntimeError(
                        'Multi-get operation interrupted by pool '
                        'stopping!')
            results.append(outq.get())
            outq.task_done()
    finally:
        if transient_pool:
            pool.stop()

    return results


def multiput(client, objs, **options):
    """Executes a parallel-store across multiple threads. Returns a list
    containing booleans or :class:`~riak.riak_object.RiakObject`

    If a ``pool`` option is included, the request will use the given worker
    pool and not a transient :class:`~riak.client.multi.MultiPutPool`. This
    option will be passed by the client if the ``multiput_pool_size``
    option was set on client initialization.

    :param client: the client to use
    :type client: :class:`RiakClient <riak.client.RiakClient>`
    :param objs: the objects to store in parallel
    :type objs: list of `RiakObject <riak.riak_object.RiakObject>` or
                `TsObject <riak.ts_object.TsObject>`
    :param options: request options to
        :meth:`RiakClient.put <riak.client.RiakClient.put>`
    :type options: dict
    :rtype: list
    """
    transient_pool = False
    outq = Queue()

    if 'pool' in options:
        pool = options['pool']
        del options['pool']
    else:
        pool = MultiPutPool()
        transient_pool = True

    try:
        pool.start()
        for obj in objs:
            task = PutTask(client, outq, obj, options)
            pool.enq(task)

        results = []
        for _ in range(len(objs)):
            if pool.stopped():
                raise RuntimeError(
                        'Multi-put operation interrupted by pool '
                        'stopping!')
            results.append(outq.get())
            outq.task_done()
    finally:
        if transient_pool:
            pool.stop()

    return results
