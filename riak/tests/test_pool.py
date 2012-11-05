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


class SimplePool(Pool):
    def __init__(self):
        self.count = 0
        Pool.__init__(self)

    def create_resource(self):
        self.count += 1
        return [self.count]


class EmptyListPool(Pool):
    def create_resource(self):
        return []


class PoolTest(unittest.TestCase):
    def test_yields_new_object_when_empty(self):
        pool = SimplePool()
        with pool.take() as element:
            self.assertEqual([1], element)

    def test_yields_same_object_in_serial_access(self):
        pool = SimplePool()

        with pool.take() as element:
            self.assertEqual([1], element)
            element.append(2)

        with pool.take() as element2:
            self.assertEqual(1, len(pool.elements))
            self.assertEqual([1, 2], element)

        self.assertEqual(1, len(pool.elements))

    def test_reentrance(self):
        pool = SimplePool()
        with pool.take() as first:
            self.assertEqual([1], first)
            with pool.take() as second:
                self.assertEqual([2], second)
                with pool.take() as third:
                    self.assertEqual([3], third)

    def test_unlocks_when_exception_raised(self):
        pool = SimplePool()
        try:
            with pool.take() as x:
                with pool.take() as y:
                    raise RuntimeError
        except:
            self.assertEqual(2, len(pool.elements))
            for e in pool.elements:
                self.assertFalse(e.claimed)

    def test_removes_bad_resource(self):
        pool = SimplePool()
        with pool.take() as element:
            self.assertEqual([1], element)
            element.append(2)
        try:
            with pool.take() as baddie:
                raise BadResource
        except BadResource:
            self.assertEqual(0, len(pool.elements))
            with pool.take() as goodie:
                self.assertEqual([2], goodie)

    def test_filter_skips_unmatching_elements(self):
        def filtereven(numlist):
            return numlist[0] % 2 == 0

        pool = SimplePool()
        with pool.take() as x:
            with pool.take() as y:
                pass

        with pool.take(_filter=filtereven) as f:
            self.assertEqual([2], f)

    def test_requires_filter_to_be_callable(self):
        badfilter = 'foo'
        pool = SimplePool()

        with self.assertRaises(TypeError):
            with pool.take(_filter=badfilter) as resource:
                pass

    def test_yields_default_when_empty(self):
        pool = SimplePool()
        with pool.take(default='default') as x:
            self.assertEqual('default', x)

    def test_thread_safety(self):
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

    def test_iteration(self):
        started = Queue()
        n = 30
        threads = []
        touched = []
        pool = EmptyListPool()
        rand = SystemRandom()

        def _run():
            psleep = rand.uniform(0, 0.75)
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

if __name__ == '__main__':
    unittest.main()
