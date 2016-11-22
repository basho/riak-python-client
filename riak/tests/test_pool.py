# -*- coding: utf-8 -*-
import unittest

from six import PY2
from threading import Thread, currentThread
from random import SystemRandom
from time import sleep

from riak import RiakError
from riak.tests import RUN_POOL
from riak.tests.comparison import Comparison
from riak.transports.pool import Pool, BadResource

if PY2:
    from Queue import Queue
else:
    from queue import Queue


class SimplePool(Pool):
    def __init__(self):
        self.count = 0
        Pool.__init__(self)

    def create_resource(self):
        self.count += 1
        return [self.count]

    def destroy_resource(self, resource):
        del resource[:]


class EmptyListPool(Pool):
    def create_resource(self):
        return []


@unittest.skipUnless(RUN_POOL, 'RUN_POOL is 0')
class PoolTest(unittest.TestCase, Comparison):

    def test_can_raise_bad_resource(self):
        ex_msg = 'exception-message!'
        with self.assertRaises(BadResource) as cm:
            raise BadResource(ex_msg)
        ex = cm.exception
        self.assertEqual(ex.args[0], ex_msg)

    def test_bad_resource_inner_exception(self):
        ex_msg = 'exception-message!'
        ex = RiakError(ex_msg)
        with self.assertRaises(BadResource) as cm:
            raise BadResource(ex)
        br_ex = cm.exception
        self.assertEqual(br_ex.args[0], ex)

    def test_yields_new_object_when_empty(self):
        """
        The pool should create new resources as needed.
        """
        pool = SimplePool()
        with pool.transaction() as element:
            self.assertEqual([1], element)

    def test_yields_same_object_in_serial_access(self):
        """
        The pool should reuse resources that already exist, when used
        serially.
        """
        pool = SimplePool()

        with pool.transaction() as element:
            self.assertEqual([1], element)
            element.append(2)

        with pool.transaction() as element2:
            self.assertEqual(1, len(pool.resources))
            self.assertEqual([1, 2], element2)

        self.assertEqual(1, len(pool.resources))

    def test_reentrance(self):
        """
        The pool should be re-entrant, that is, yield new resources
        while one is already claimed in the same code path.
        """
        pool = SimplePool()
        with pool.transaction() as first:
            self.assertEqual([1], first)
            with pool.transaction() as second:
                self.assertEqual([2], second)
                with pool.transaction() as third:
                    self.assertEqual([3], third)

    def test_unlocks_when_exception_raised(self):
        """
        The pool should unlock all resources that were previously
        claimed when an exception occurs.
        """
        pool = SimplePool()
        try:
            with pool.transaction():
                with pool.transaction():
                    raise RuntimeError
        except:
            self.assertEqual(2, len(pool.resources))
            for e in pool.resources:
                self.assertFalse(e.claimed)

    def test_removes_bad_resource(self):
        """
        The pool should remove resources that are considered bad by
        user code throwing a BadResource exception.
        """
        pool = SimplePool()
        with pool.transaction() as resource:
            self.assertEqual([1], resource)
            resource.append(2)
        try:
            with pool.transaction():
                raise BadResource
        except BadResource:
            self.assertEqual(0, len(pool.resources))
            with pool.transaction() as goodie:
                self.assertEqual([2], goodie)

    def test_filter_skips_unmatching_resources(self):
        """
        The _filter parameter should cause the pool to yield the first
        unclaimed resource that passes the filter.
        """
        def filtereven(numlist):
            return numlist[0] % 2 == 0

        pool = SimplePool()
        with pool.transaction():
            with pool.transaction():
                pass

        with pool.transaction(_filter=filtereven) as f:
            self.assertEqual([2], f)

    def test_requires_filter_to_be_callable(self):
        """
        The _filter parameter should be required to be a callable, or
        None.
        """
        badfilter = 'foo'
        pool = SimplePool()

        with self.assertRaises(TypeError):
            with pool.transaction(_filter=badfilter):
                pass

    def test_yields_default_when_empty(self):
        """
        The pool should yield the given default when no existing
        resources are free.
        """
        pool = SimplePool()
        with pool.transaction(default='default') as x:
            self.assertEqual('default', x)

    def test_manual_release(self):
        """
        The pool should allow resources to be acquired and released
        manually, without giving them out twice.
        """
        pool = SimplePool()
        a = pool.acquire()
        self.assertEqual([1], a.object)
        with pool.transaction() as b:
            self.assertEqual([2], b)
        with pool.transaction() as c:
            self.assertEqual([2], c)
        pool.release(a)
        with pool.transaction() as d:
            self.assertEqual([1], d)
        e = pool.acquire()
        with pool.transaction() as f:
            self.assertEqual([2], f)
        e.release()
        with pool.transaction() as g:
            self.assertEqual([1], g)

    def test_thread_safety(self):
        """
        The pool should allocate n objects for n concurrent operations.
        """
        n = 10
        pool = EmptyListPool()
        readyq = Queue()
        finishq = Queue()
        threads = []

        def _run():
            with pool.transaction() as resource:
                readyq.put(1)
                resource.append(currentThread())
                finishq.get(True)
                finishq.task_done()

        for i in range(n):
            th = Thread(target=_run)
            threads.append(th)
            th.start()

        for i in range(n):
            readyq.get()
            readyq.task_done()

        for i in range(n):
            finishq.put(1)

        for thr in threads:
            thr.join()

        self.assertEqual(n, len(pool.resources))
        for resource in pool.resources:
            self.assertFalse(resource.claimed)
            self.assertEqual(1, len(resource.object))
            self.assertIn(resource.object[0], threads)

    def test_iteration(self):
        """
        Iteration over the pool resources, even when some are claimed,
        should eventually touch all resources (excluding ones created
        during iteration).
        """

        for i in range(25):
            started = Queue()
            n = 1000
            threads = []
            touched = []
            pool = EmptyListPool()
            rand = SystemRandom()

            def _run():
                psleep = rand.uniform(0.05, 0.1)
                with pool.transaction() as a:
                    started.put(1)
                    started.join()
                    a.append(rand.uniform(0, 1))
                    sleep(psleep)

            for i in range(n):
                th = Thread(target=_run)
                threads.append(th)
                th.start()

            for i in range(n):
                started.get()
                started.task_done()

            for resource in pool:
                touched.append(resource)

            for thr in threads:
                thr.join()

            self.assertItemsEqual(pool.resources, touched)

    def test_clear(self):
        """
        Clearing the pool should remove all resources known at the
        time of the call.
        """
        n = 10
        startq = Queue()
        finishq = Queue()
        rand = SystemRandom()
        threads = []
        pusher = None
        pool = SimplePool()

        def worker_run():
            with pool.transaction():
                startq.put(1)
                startq.join()
                sleep(rand.uniform(0, 0.5))
                finishq.get()
                finishq.task_done()

        def pusher_run():
            for i in range(n):
                finishq.put(1)
                sleep(rand.uniform(0, 0.1))
            finishq.join()

        # Allocate 10 resources in the pool by spinning up 10 threads
        for i in range(n):
            th = Thread(target=worker_run)
            threads.append(th)
            th.start()

        # Pull everything off the queue, allowing the workers to run
        for i in range(n):
            startq.get()
            startq.task_done()

        # Start the pusher that will allow them to proceed and exit
        pusher = Thread(target=pusher_run)
        threads.append(pusher)
        pusher.start()

        # Clear the pool
        pool.clear()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        # Make sure that the pool resources are gone
        self.assertEqual(0, len(pool.resources))

    def test_stress(self):
        """
        Runs a large number of threads doing operations with resources
        checked out, ensuring properties of the pool.
        """
        rand = SystemRandom()
        n = rand.randint(1, 400)
        passes = rand.randint(1, 20)
        rounds = rand.randint(1, 200)
        breaker = rand.uniform(0, 1)
        pool = EmptyListPool()

        def _run():
            for i in range(rounds):
                with pool.transaction() as a:
                    self.assertEqual([], a)
                    a.append(currentThread())
                    self.assertEqual([currentThread()], a)

                    for p in range(passes):
                        self.assertEqual([currentThread()], a)
                        if rand.uniform(0, 1) > breaker:
                            break

                    a.remove(currentThread())

        threads = []

        for i in range(n):
            th = Thread(target=_run)
            threads.append(th)
            th.start()

        for th in threads:
            th.join()


if __name__ == '__main__':
    unittest.main()
