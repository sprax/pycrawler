"""
A specialized form of of a priority queue built around python's heapq
module and different from Queue.PriorityQueue.
"""
# $Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
import Queue as RegularQueue
from heapq import heappush, heappop

class Queue(RegularQueue.Queue):
    """
**This might NOT be threadsafe.**

This is an implementation of priority queue with few additional
methods beyond what python 2.6 provides in Queue.PriorityQueue

PriorityQueue is a subclass of the Queue module.  It has the same
attributes (Empty, Full, Queue), but the items that you put/get from
the Queue are tuples of

     (priority_number, item)

This uses heapq and is reasonably fast.  

All the inputs and return values of the Queue are two-tuples.  This is
true even when the Queue is empty, i.e. calls to get(), top(),
getif(), get_nowait() return a two-tuple of (None, None).

Queue also has many of the builtin properties of lists, such as
__len__ and __iter__, so you can say things like:

q = Queue()
for x in range(1000)
    q.put(x)
assert len(q) == 1000
for x in q:
    print x

    """
    def _init(self, maxsize=0):
        """Setup a regular Queue class, put replace the queue with a
        list, so we can use the heapq."""
        RegularQueue.Queue._init(self, maxsize)
        self.queue = []
        for func in ('__contains__', '__delitem__', '__getitem__',
                     '__iter__', '__len__', '__setitem__', 'remove'):
            setattr(self, func, getattr(self.queue, func))

    def _put(self, item):
        """Put item in the heap-based queue"""
        return heappush(self.queue, item)

    def _get(self):
        """Get item from the heap-based queue"""
        return heappop(self.queue)

    def top(self):
        """non-destructively return a highest-priority two-tuple of
        (priority, item)"""
        try:
            x = self.queue[0]
        except IndexError:
            x = (None, None)
        return x

    def getif(self, maxP):
        """This is non-blocking.  If the highest priority is less than
        maxP, then destructively return a highest priority two-tuple
        of (priority, item)"""
        top = self.top()
        if top[0] is not None and top[0] <= maxP:
            return self.get()
        else:
            return (None, None)

if __name__ == "__main__":
    """
    Tests for the PriorityQueue.Queue
    """
    import time
    from random import random
    q = Queue()
    count = 10**4
    start = time.time()
    for i in xrange(count):
        q.put((random(), i))
    end = time.time()
    print "%.0f put/sec" % (count / (end-start))

    start = time.time()
    p = 0.
    returned_count = 0
    while not q.empty():
        n = q.get()[0]
        assert(n >= p)
        p = n
        returned_count += 1
    end = time.time()
    assert(returned_count == count)
    print "%.0f get/sec" % (count / (end-start))

    q = Queue(10)
    for i in range(10):
        q.put((random(), i))
    try:
        q.put_nowait((random(), i))
    except RegularQueue.Full, e:
        print "successfully limited size of the PriorityQueue.Queue"

    # clear the q
    while not q.empty():
        q.get()

    q.put((.5, "a"))
    p, i = q.getif(.4)
    assert p is None, "got a priority below maxP!  %s !< %s" % (p, .4)
    p, i = q.getif(.6)
    assert p <= .6, "got a priority above maxP!  %s !<= %s" % (p, .6)
    
    a = q.getif(100)
    assert a == (None, None), "getif on empty Queue gave wrong answer: " + str(a)

    print "PriorityQueue.Queue.getif works."

    q = Queue()
    import threading
    from time import sleep
    def feeder():
        i = 0
        global continue_processing, q
        while continue_processing:
            i += 1
            try:
                q.put_nowait((random(), i))
            except RegularQueue.Full:
                sleep(1)
    def eater():
        last = 0.
        next = 0.
        global continue_processing, q
        while continue_processing:
            try:
                next = q.get_nowait()
            except RegularQueue.Empty:
                sleep(1)
            assert last <= next, "wrong order!"
    threads = []
    global continue_processing
    continue_processing = True
    # launching many threads
    for i in xrange(10):
        th = threading.Thread(target=feeder)
        th.start()
        threads.append(th)
    for i in xrange(10):
        th = threading.Thread(target=eater)
        th.start()
        threads.append(th)
    c = 0
    while c<5:
        c += 1
        print q.qsize()
        sleep(1)
    continue_processing = False
    for th in threads:
        th.join()
    while 1:
        #print q.qsize()
        try:
            next = q.get_nowait()
        except RegularQueue.Empty:
            break        
    print "appears threadsafe too"

    class Test:
        pass

    q = Queue()
    tot = 0
    for x in range(1000):
        t = Test()
        t.n = x
        q.put(t)
        tot += x
    assert t in q, "__getitem__ failed"
    assert len(q) == 1000, "__len__ failed"
    totn = 0
    for x in q:
        totn += x.n
    assert tot == totn, "__iter__ failed"

    print "If you got here without a traceback, then the tests passed."
    import sys
    sys.exit()
