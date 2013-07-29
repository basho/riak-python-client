#!/usr/bin/env python

from Queue import Queue
from threading import Thread
import sys
sys.path.append("../transports/")
from pool import Pool
from random import SystemRandom
from time import sleep


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


def test():
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
            if psleep > 1:
                print psleep
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

    if set(pool.elements) != set(touched):
        print set(pool.elements) - set(touched)
        return False
    else:
        return True

ret = True
count = 0
while ret:
    ret = test()
    count += 1
    print count


# INSTRUMENTED FUNCTION

#     def __claim_elements(self):
#         #print 'waiting for self lock'
#         with self.lock:
#             if self.__all_claimed(): # and self.unlocked:
#                 #print 'waiting on releaser lock'
#                 with self.releaser:
#                     print 'waiting for release'
#                     print 'targets', self.targets
#                     print 'tomb', self.targets[0].tomb
#                     print 'claimed', self.targets[0].claimed
#                     print self.releaser
#                     print self.lock
#                     print self.unlocked
#                     self.releaser.wait(1)
#             for element in self.targets:
#                 if element.tomb:
#                     self.targets.remove(element)
#                     #self.unlocked.remove(element)
#                     continue
#                 if not element.claimed:
#                     self.targets.remove(element)
#                     self.unlocked.append(element)
#                     element.claimed = True
