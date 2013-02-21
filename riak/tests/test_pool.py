"""
Copyright 2012 Basho Technologies, Inc.

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

import platform
from Queue import Queue
from threading import Thread, currentThread
from riak.transports.pool import Pool, BadResource
from random import SystemRandom
from time import sleep

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest
import os


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


@unittest.skipIf(os.environ.get('SKIP_POOL'),
                 'Skipping connection pool tests')
class PoolTest(unittest.TestCase):
    def test_yields_new_object_when_empty(self):
        """
        The pool should create new resources as needed.
        """
        pool = SimplePool()
        with pool.take() as element:
            self.assertEqual([1], element)

    def test_yields_same_object_in_serial_access(self):
        """
        The pool should reuse resources that already exist, when used
        serially.
        """
        pool = SimplePool()

        with pool.take() as element:
            self.assertEqual([1], element)
            element.append(2)

        with pool.take() as element2:
            self.assertEqual(1, len(pool.elements))
            self.assertEqual([1, 2], element2)

        self.assertEqual(1, len(pool.elements))

    def test_reentrance(self):
        """
        The pool should be re-entrant, that is, yield new resources
        while one is already claimed in the same code path.
        """
        pool = SimplePool()
        with pool.take() as first:
            self.assertEqual([1], first)
            with pool.take() as second:
                self.assertEqual([2], second)
                with pool.take() as third:
                    self.assertEqual([3], third)

    def test_unlocks_when_exception_raised(self):
        """
        The pool should unlock all resources that were previously
        claimed when an exception occurs.
        """
        pool = SimplePool()
        try:
            with pool.take():
                with pool.take():
                    raise RuntimeError
        except:
            self.assertEqual(2, len(pool.elements))
            for e in pool.elements:
                self.assertFalse(e.claimed)

    def test_removes_bad_resource(self):
        """
        The pool should remove resources that are considered bad by
        user code throwing a BadResource exception.
        """
        pool = SimplePool()
        with pool.take() as element:
            self.assertEqual([1], element)
            element.append(2)
        try:
            with pool.take():
                raise BadResource
        except BadResource:
            self.assertEqual(0, len(pool.elements))
            with pool.take() as goodie:
                self.assertEqual([2], goodie)

    def test_filter_skips_unmatching_elements(self):
        """
        The _filter parameter should cause the pool to yield the first
        unclaimed resource that passes the filter.
        """
        def filtereven(numlist):
            return numlist[0] % 2 == 0

        pool = SimplePool()
        with pool.take():
            with pool.take():
                pass

        with pool.take(_filter=filtereven) as f:
            self.assertEqual([2], f)

    def test_requires_filter_to_be_callable(self):
        """
        The _filter parameter should be required to be a callable, or
        None.
        """
        badfilter = 'foo'
        pool = SimplePool()

        with self.assertRaises(TypeError):
            with pool.take(_filter=badfilter):
                pass

    def test_yields_default_when_empty(self):
        """
        The pool should yield the given default when no existing
        resources are free.
        """
        pool = SimplePool()
        with pool.take(default='default') as x:
            self.assertEqual('default', x)

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
            with pool.take() as resource:
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

        self.assertEqual(n, len(pool.elements))
        for element in pool.elements:
            self.assertFalse(element.claimed)
            self.assertEqual(1, len(element.object))
            self.assertIn(element.object[0], threads)

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
                with pool.take() as a:
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

            for element in pool:
                touched.append(element)

            for thr in threads:
                thr.join()

            self.assertItemsEqual(pool.elements, touched)

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
            with pool.take():
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
        self.assertEqual(0, len(pool.elements))

    def test_stress(self):
        """
        Runs a large number of threads doing operations with elements
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
                with pool.take() as a:
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
