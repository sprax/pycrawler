"""
Tests for PersistentQueue.PersistentQueue and PersistentQueue.TriQueue
"""
#$Id: $
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import os
import sys
import Queue
import shutil
import random
import traceback
import multiprocessing
from time import sleep, time
from syslog import syslog, openlog, LOG_INFO, LOG_DEBUG, LOG_NOTICE, LOG_NDELAY, LOG_CONS, LOG_PID, LOG_LOCAL0
from signal import signal, alarm, SIGALRM, SIGHUP, SIGINT, SIGQUIT, SIGABRT, SIGTERM, SIGPIPE, SIG_IGN
from optparse import OptionParser

sys.path.insert(0, os.getcwd())
import PersistentQueue

class MyND1(PersistentQueue.nameddict):
    _defaults = {"a": None}
    _key_ordering = ["a"]
    _val_types = [float]
    _sort_key = 0

class MyND2(PersistentQueue.nameddict):
    _defaults = {"a": None}
    _key_ordering = ["a"]
    _val_types = [float]
    _sort_key = 0
    @classmethod
    def accumulator(cls, acc_state, line):
        "do nothing"
        return None, line

def test_mutex():
    print "Testing mutex..."
    sema1 = PersistentQueue.Mutex("foo/test_mutex")
    sema1.acquire()
    sema2 = PersistentQueue.Mutex("foo/test_mutex")
    print "\tattempting to get lock without blocking"
    acquired = sema2.acquire(block=False)
    assert acquired == False
    print "\tsuccess"
    sema1.release()
    print "\tattempting to acquire"
    acquired = sema2.acquire()
    assert acquired == True
    print "\tsuccess"
    sema2.release()
    sema1.acquire()
    sema2.release()
    sema1.release()
    os.remove("foo/test_mutex")
    os.rmdir("foo")
    print "\tpassed!"

def speed_test(data_path, ELEMENTS=50000, p=None, compress=True):
    """run speed tests and average speeds of put and get"""
    if p is None:
        p = PersistentQueue.PersistentQueue(data_path, 10, MyND1, compress=compress)
    start = time()
    for a in range(ELEMENTS):
        p.put(MyND1({"a": a}))
    p.sync()
    end = time()
    elapsed = start - end 
    print "put() --> (%d rec / %.3f sec) = %.3f rec/sec" % (ELEMENTS, elapsed, ELEMENTS/elapsed)
    start = time()
    for a in range(ELEMENTS):
        p.get()
    end = time()
    elapsed = end - start 
    print "get() --> (%d rec / %.3f sec) = %.3f rec/sec" % (ELEMENTS, elapsed, ELEMENTS/elapsed)
    p.sync()
    p.close()

def basic_test(data_path, ELEMENTS=1000, p=None, compress=True):
    """run basic tests"""
    if p is None:
        p = PersistentQueue.PersistentQueue(
            data_path, 10, compress=compress,
            marshal=MyND1)
    print "Enqueueing %d items, cache size = %d" % \
        (ELEMENTS, p._cache_size)
    for a in range(ELEMENTS):
        p.put(MyND1({"a": a}))
    p.sync()
    assert ELEMENTS == len(p), \
        "Put %d elements in, but lost some?" % ELEMENTS
    print "Queue length (using __len__):", len(p)
    print "Dequeueing %d items" % (ELEMENTS/2)
    out = []
    for a in range(ELEMENTS/2):
        out.append(p.get())
    print "Queue length (using __len__):", len(p)
    print "Dequeueing %d items" % (ELEMENTS/2)
    for a in range(ELEMENTS/2):
        out.append(p.get())
    print "Queue length (using __len__):", len(p)
    out_str = [x.a for x in out]
    answer_str = [float(x) for x in range(ELEMENTS)]
    assert out_str == answer_str, \
        "Got out different list than put in: \n%s\n%s" % \
        (out_str, answer_str)
    p.sync()
    assert len(p) == 0, "failed to have an empty queue after removing all items"
    try:
        a = p.get()
        assert False, "there are still items left in queue!: %s" % a
    except Queue.Empty:
        pass
    p.close()

def sort_test(data_path, ELEMENTS=1000, p=None, compress=False, compress_temps=False):
    """run sort tests"""
    print "Running test on sorting with %d elements" % ELEMENTS
    if p is None:
        # use the do nothing accumulator
        p = PersistentQueue.PersistentQueue(data_path, 10, MyND2, compress=compress)        
    # define an answer
    answer = range(ELEMENTS)
    # randomize it before putting into queue
    randomized = []
    for i in range(len(answer)):
        element = random.choice(answer)
        answer.remove(element)
        randomized.append(element)
    # put it in the queue
    for a in randomized:
        p.put(MyND1({"a": a}))
    print "put %d elements into the queue in random order" % len(randomized)
    # sync but do not close
    p.sync()
    # this could take time
    start = time()
    ret = p.sort(compress_temps, numerical=True)
    end = time()
    assert ret is True, "PersistentQueue.PersistentQueue.sort failed with ret = " + str(ret)
    print "head = %d, tail = %d" % (p._head, p._tail)
    print "index_file: %s" % open(p._index_file).read()
    elapsed = end - start
    rate = elapsed and (ELEMENTS / elapsed) or 0.0
    # get the response and compare with answer
    answer = range(ELEMENTS)
    vals = []
    for a in range(len(answer)):
        vals.append(int(p.get().a))
    assert len(vals) == len(answer), \
        "Got back different number of results" + \
        "(%d) than expected (%d)" % (len(vals), len(answer))
    assert vals == answer, "Incorrectly sorted result:\nvals  : %s\nanswer: %s" % (vals, answer)
    for i in range(len(vals)-1):
        assert vals[i] <= vals[i+1], "Incorrectly sorted result:\nvals: %s\nanswer: %s" % (vals, answer)
    print "Sorting succeeded.  Sort took %s seconds, %.2f records/second" \
        % (elapsed, rate)
    assert len(p) == 0, "failed to have an empty queue after removing all items"
    try:
        a = p.get()
        assert False, "there are still items left in queue!: %s" % a
    except Queue.Empty:
        pass
    p.close()

def merge_test(data_path, ELEMENTS=1000):
    """run sort tests"""
    num_queues = 4
    print "Running test on merging with %d elements from %d queues" \
        % (ELEMENTS, num_queues)
    # use the do nothing accumulator
    p = PersistentQueue.PersistentQueue(data_path, 10, MyND2)
    queues = [p]
    for i in range(num_queues - 1):
        queues.append(
            PersistentQueue.PersistentQueue(data_path + "/%d" % i, 10, MyND1))
    # define an answer
    answer = range(ELEMENTS)
    # randomize it before putting into queue
    randomized = []
    for i in range(len(answer)):
        element = random.choice(answer)
        answer.remove(element)
        randomized.append(element)
    # put it in the queue
    for a in randomized:
        # pick a queue at random
        pq = queues[int(random.random() * len(queues))]
        p.put(MyND1({"a": a}))
    # sync but do not close
    for pq in queues:
        pq.sync()
    # this could take time
    start = time()
    p.sort(merge_from=queues)
    end = time()
    elapsed = end - start
    rate = elapsed and (ELEMENTS / elapsed) or 0.0
    # get the response and compare with answer
    answer = range(ELEMENTS)
    vals = []
    for a in range(len(answer)):
        vals.append(int(p.get().a))
    assert vals == answer, "Incorrectly sorted result:\n%s" % vals
    for i in range(len(vals)-1):
        assert vals[i] <= vals[i+1], "Incorrectly sorted result:\n%s" % vals
    print "Sorting succeeded.  Sort took %s seconds, %.2f records/second" \
        % (elapsed, rate)
    p.close()
    for pq in queues:
        assert len(pq) == 0, "Should not have any items left in merge_from queues"
        pq.close()

class PersistentQueueContainer(multiprocessing.Process):
    def __init__(self, id, go, data_path, marshal):
        self.name = id
        multiprocessing.Process.__init__(self, name=self.name)
        self.go = go or multiprocessing.Event()
        self.data_path = data_path
        self.marshal = marshal
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)
        self._go = multiprocessing.Event()
        self._go.set()
        self.queue = None
    def close(self):
        self._go.clear()
    def run(self):
        """
        A simple test of using a PersistentQueue.PersistentQueue inside a
        multiprocessing.Process
        """
        syslog("Starting")
        self.queue = PersistentQueue.PersistentQueue(
            self.data_path, compress=True, marshal=self.marshal)
        while self.go.is_set() and self._go.is_set():
            sleep(1)
        syslog("syncing before closing")
        self.queue.sync()
        self.queue.close()
        syslog("Done.")
        self.stop()
    def stop(self):
        syslog(LOG_DEBUG, "Stop called.")
        self.go.clear()

def process_test(data_path, ELEMENTS):
    pqc = PersistentQueueContainer(data_path, None, data_path, MyND1)
    pqc.start()
    sleep(1)
    speed_test(data_path, ELEMENTS, p=pqc.queue)
    basic_test(data_path, ELEMENTS, p=pqc.queue)
    pqc.close()

def validate(data_path, compress=False, marshal=MyND1):
    """
    Prints diagnostics about the queue found at data_path
    """
    try:
        queue = PersistentQueue.PersistentQueue(data_path, compress=compress, marshal=marshal)
    except Exception, exc:
        queue = None
        print "Failed to instantiate PersistentQueue(%s)\n\nbecause:\n%s" \
            % (data_path, traceback.format_exc(exc))
    if queue is not None:
        print "Attached to a queue of length: %d" % len(queue)
        queue.close()

def triqueue_test(data_path, ELEMENTS=1000):
    syslog("starting triqueue_test")
    if os.path.exists(data_path):
        shutil.rmtree(data_path)
    tq = PersistentQueue.TriQueue(data_path, marshal=MyND1)
    for i in range(1000):
        v = random.random()
        tq.put(MyND1({"a": v}))
    print "inQ has\t\t%d" % len(tq._inQ)
    print "readyQ has\t%d" % len(tq._readyQ)
    print "pendingQ has\t%d" % len(tq._pendingQ)
    merger = tq.sync()
    print "Merger.is_alive() --> " + str(merger.is_alive())
    print "inQ has\t\t%d" % len(tq._inQ)
    print "readyQ has\t%d" % len(tq._readyQ)
    log("pendingQ has\t%d" % len(tq._pendingQ))
    i = 0
    tq.acquire()
    while i < 500:
        try:
            #start = time()
            val = tq.get_nowait()
            #end = time()
            #log("%.3f seconds per get_nowait" % (end - start))
            val = val.a
            #log("Got a result: %s" % val)
            i += 1
        except PersistentQueue.TriQueue.Blocked:
            log("Waiting for PersistentQueue.TriQueue to unblock, Merger.is_alive() --> " + str(merger.is_alive()))
            sleep(1)
        except Queue.Empty:
            log("Waiting for results to appear in queue")
            sleep(1)
        except PersistentQueue.TriQueue.Syncing:
            log("triqueue_test: Waiting for merged results, Merger.is_alive() --> " + str(merger.is_alive()))
            sleep(1)
    log("Done getting results.  Now closing.")
    tq.release()
    tq.close()
    log("Test complete.")

def triqueue_sort_test(data_path, ELEMENTS=1000):
    if os.path.exists(data_path):
        shutil.rmtree(data_path)
    tq = PersistentQueue.TriQueue(data_path, marshal=MyND1)
    for i in range(1000):
        v = [str(random.random()), str(random.random())]
        tq.put(MyND1({"a": v}))
    merger = tq.sync()
    i = 0
    while i < 500:
        try:
            val = tq.get_nowait()
            val = val.a
            #log("Got a result: %s" % val)
            i += 1
        except PersistentQueue.TriQueue.Blocked:
            log("Waiting for PersistentQueue.TriQueue to unblock")
            sleep(1)
        except Queue.Empty:
            log("Waiting for results to appear in queue")
            sleep(1)
        except PersistentQueue.TriQueue.Syncing:
            log("triqueue_sort_test: Waiting for merged results")
            sleep(1)
    print "Done getting results.  Now closing."
    tq.close()
    print "Test complete."

def exceptions_tests():
    try:
        raise PersistentQueue.PersistentQueue.NotYet
    except Exception, exc:
        assert str(type(exc)) == "<class 'PersistentQueue.PersistentQueue.NotYet'>", \
            "Failed to raise the correct exception: <class 'PersistentQueue.PersistentQueue.NotYet'> != " + str(type(exc))
    try:
        raise PersistentQueue.TriQueue.Syncing
    except Exception, exc:
        assert str(type(exc)) == "<class 'PersistentQueue.TriQueue.Syncing'>", \
            "Failed to raise the correct exception: <class 'PersistentQueue.TriQueue.Syncing'> != " + str(type(exc))
    try:
        raise PersistentQueue.TriQueue.Blocked
    except Exception, exc:
        assert str(type(exc)) == "<class 'PersistentQueue.TriQueue.Blocked'>", \
            "Failed to raise the correct exception: <class 'PersistentQueue.TriQueue.Blocked'> != " + str(type(exc))
    try:
        raise PersistentQueue.TriQueue.ReadyToSync
    except Exception, exc:
        assert str(type(exc)) == "<class 'PersistentQueue.TriQueue.ReadyToSync'>", \
            "Failed to raise the correct exception: <class 'PersistentQueue.TriQueue.ReadyToSync'> != " + str(type(exc))
    print "Raised all exceptions correctly"

def log(msg):
    print msg
    sys.stdout.flush()

def rmdir(dir):
    if os.path.exists(dir):
        try:
            shutil.rmtree(dir)
        except Exception, exc:
            print "Did not rmtree the dir. " + str(exc)

if __name__ == "__main__":
    parser = OptionParser(description="runs tests for PersistentQueue.  Default runs all tests.")
    parser.add_option("-n", "--num", dest="num", default=1000, type=int, help="num items to put/get in tests")
    parser.add_option("--dir", dest="dir", default="data_test_dir", help="path for dir to use in tests")
    parser.add_option("--validate", dest="validate", default=False, action="store_true", help="validate an existing PersistentQueue")
    parser.add_option("--mutex", dest="mutex", default=False, action="store_true", help="run test of file-based Mutex")
    parser.add_option("--basic", dest="basic", default=False, action="store_true", help="run basic test")
    parser.add_option("--speed", dest="speed", default=False, action="store_true", help="run speed test")
    parser.add_option("--process", dest="process", default=False, action="store_true", help="run test of running PersistentQueue inside a multiprocessing.Process")
    parser.add_option("--triqueue", dest="triqueue", default=False, action="store_true", help="run the PersistentQueue.TriQueue tests")
    parser.add_option("--sort", dest="sort", default=False, action="store_true", help="run test of sorting")
    parser.add_option("--merge", dest="merge", default=False, action="store_true", help="run test of sorted merging of multiple queues")
    parser.add_option("--exceptions", dest="exceptions", default=False, action="store_true", help="run tests of exceptions that can be raised")
    parser.add_option("--keep", dest="keep", default=False, action="store_true", help="keep the data dir after the test")
    (options, args) = parser.parse_args()

    if options.validate:
        validate(options.dir)
        sys.exit()

    rmdir(options.dir)
    if options.mutex:
        test_mutex()
    elif options.basic:
        basic_test(options.dir, options.num)
    elif options.speed:
        speed_test(options.dir, options.num)
    elif options.process:
        process_test(options.dir, options.num)
    elif options.sort:
        rmdir(options.dir)
        sort_test(options.dir, options.num, compress=False, compress_temps=False)
        rmdir(options.dir)
        sort_test(options.dir, options.num, compress=False, compress_temps=True)
        rmdir(options.dir)
        sort_test(options.dir, options.num, compress=True, compress_temps=False)
        rmdir(options.dir)
        sort_test(options.dir, options.num, compress=True, compress_temps=True)
        rmdir(options.dir)
    elif options.merge:
        merge_test(options.dir, options.num)
    elif options.triqueue:
        triqueue_test(options.dir, options.num)
    elif options.exceptions:
        exceptions_tests()
    else:
        test_mutex()
        basic_test(options.dir, options.num)
        rmdir(options.dir)        
        speed_test(options.dir, options.num)
        rmdir(options.dir)        
        process_test(options.dir, options.num)
        rmdir(options.dir)
        sort_test(options.dir, options.num)
        rmdir(options.dir)
        merge_test(options.dir, options.num)
        rmdir(options.dir)
        triqueue_test(options.dir, options.num)

        exceptions_tests()
        
    if not options.keep:
        rmdir(options.dir)
