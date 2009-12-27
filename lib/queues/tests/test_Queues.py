"""
Tests for PersistentQueue.Queue and PersistentQueue.TriQueue
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

def speed_test(data_path, ELEMENTS=50000, p=None, lines=False, compress=True):
    """run speed tests and average speeds of put and get"""
    if p is None:
        if lines:
            p = PersistentQueue.Queue(data_path, 10, PersistentQueue.LineFiles, compress=compress)
        else:
            p = PersistentQueue.Queue(data_path, 10, compress=compress)
    start = time()
    for a in range(ELEMENTS):
        p.put(str(a))
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
        p = PersistentQueue.Queue(data_path, 10, compress=compress)
    print "Enqueueing %d items, cache size = %d" % \
        (ELEMENTS, p.cache_size)
    for a in range(ELEMENTS):
        p.put(str(a))
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
    assert out == [str(x) for x in range(ELEMENTS)], \
        "Got out different list than put in: \n%s\n%s" % \
        (out, range(ELEMENTS))
    p.sync()
    p.close()

def lines_test(data_path, ELEMENTS=1000, p=None, compress=True):
    """run basic tests"""
    if p is None:
        p = PersistentQueue.Queue(data_path, 10, PersistentQueue.LineFiles, compress=compress)
    print "Enqueueing %d items, cache size = %d" % \
        (ELEMENTS, p.cache_size)
    for a in range(ELEMENTS):
        p.put(str(a))
    p.sync()
    print "Queue length (using __len__):", len(p)
    print "Dequeueing %d items" % (ELEMENTS/2)
    for a in range(ELEMENTS/2):
        p.get()
    print "Queue length (using __len__):", len(p)
    print "Dequeueing %d items" % (ELEMENTS/2)
    for a in range(ELEMENTS/2):
        p.get()
    print "Queue length (using __len__):", len(p)
    p.sync()
    p.close()

def sort_test(data_path, ELEMENTS=1000, p=None, compress=False, compress_temps=False):
    """run sort tests"""
    print "Running test on sorting with %d elements" % ELEMENTS
    if p is None:
        p = PersistentQueue.Queue(data_path, 10, PersistentQueue.LineFiles, compress=compress)        
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
        p.put([str(a)])
    print "put %d elements into the queue in random order" % len(randomized)
    # sync but do not close
    p.sync()
    # this could take time
    start = time()
    ret = p.sort(compress_temps)
    end = time()
    assert ret is True, "PersistentQueue.Queue.sort failed with ret = " + str(ret)
    elapsed = end - start
    rate = elapsed and (ELEMENTS / elapsed) or 0.0
    # get the response and compare with answer
    answer = range(ELEMENTS)
    vals = []
    for a in range(len(answer)):
        vals.append(int(p.get()[0]))
    assert len(vals) == len(answer), \
        "Got back different number of results" + \
        "(%d) than expected (%d)" % (len(vals), len(answer))
    assert vals == answer, "Incorrectly sorted result:\nvals  : %s\nanswer: %s" % (vals, answer)
    for i in range(len(vals)-1):
        assert vals[i] <= vals[i+1], "Incorrectly sorted result:\nvals: %s\nanswer: %s" % (vals, answer)
    print "Sorting succeeded.  Sort took %s seconds, %.2f records/second" \
        % (elapsed, rate)
    p.close()

def merge_test(data_path, ELEMENTS=1000):
    """run sort tests"""
    num_queues = 4
    print "Running test on merging with %d elements from %d queues" \
        % (ELEMENTS, num_queues)
    p = PersistentQueue.Queue(data_path, 10, PersistentQueue.LineFiles)
    queues = [p]
    for i in range(num_queues - 1):
        queues.append(
            PersistentQueue.Queue(data_path + "/%d" % i, 10, PersistentQueue.LineFiles))
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
        pq.put([str(a)])
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
        vals.append(int(p.get()[0]))
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
    def __init__(self, id, go, data_path):
        self.name = id
        multiprocessing.Process.__init__(self, name=self.name)
        self.go = go or multiprocessing.Event()
        self.data_path = data_path
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)
        self._go = multiprocessing.Event()
        self._go.set()
        self.queue = None
    def close(self):
        self._go.clear()
    def run(self):
        """
        A simple test of using a PersistentQueue.Queue inside a
        multiprocessing.Process
        """
        syslog("Starting")
        self.queue = PersistentQueue.Queue(self.data_path, compress=True)
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
    pqc = PersistentQueueContainer(data_path, None, data_path)
    pqc.start()
    sleep(1)
    speed_test(data_path, ELEMENTS, p=pqc.queue)
    basic_test(data_path, ELEMENTS, p=pqc.queue)
    pqc.close()

def validate(data_path, compress=False, marshal=PersistentQueue.LineFiles):
    """
    Prints diagnostics about the queue found at data_path
    """
    try:
        queue = PersistentQueue.Queue(data_path, compress=compress, marshal=marshal)
    except Exception, exc:
        queue = None
        print "Failed to instantiate PersistentQueue(%s)\n\nbecause:\n%s" \
            % (data_path, traceback.format_exc(exc))
    if queue is not None:
        print "Attached to a queue of length: %d" % len(queue)
        queue.close()

def triqueue_test(data_path, ELEMENTS=1000):
    if os.path.exists(data_path):
        shutil.rmtree(data_path)
    tq = PersistentQueue.TriQueue(data_path)
    for i in range(1000):
        v = str(random.random())
        tq.put([v])
    print "inQ has\t\t%d" % len(tq.inQ)
    print "readyQ has\t%d" % len(tq.readyQ)
    print "pendingQ has\t%d" % len(tq.pendingQ)
    merger = tq.sync()
    print "Merger.is_alive() --> " + str(merger.is_alive())
    print "inQ has\t\t%d" % len(tq.inQ)
    print "readyQ has\t%d" % len(tq.readyQ)
    print "pendingQ has\t%d" % len(tq.pendingQ)
    i = 0
    while i < 500:
        try:
            val = float(tq.get_nowait()[0])
            #print "Got a result: %s" % val
            sys.stdout.flush()
            i += 1
        except Blocked:
            print "Waiting for PersistentQueue.TriQueue to unblock"
            sleep(1)
        except Queue.Empty:
            print "Waiting for results to appear in queue"
            sleep(1)
        except Syncing:
            print "Waiting for merged results"
            sleep(1)
    print "Done getting results.  Now closing."
    tq.close()
    print "Test complete."

def triqueue_sort_test(data_path, ELEMENTS=1000):
    if os.path.exists(data_path):
        shutil.rmtree(data_path)
    tq = PersistentQueue.TriQueue(data_path)
    for i in range(1000):
        v = [str(random.random()), str(random.random())]
        tq.put(v)
    merger = tq.sync()
    i = 0
    while i < 500:
        try:
            val = float(tq.get_nowait()[0][0])
            #print "Got a result: %s" % val
            sys.stdout.flush()
            i += 1
        except Blocked:
            print "Waiting for PersistentQueue.TriQueue to unblock"
            sleep(1)
        except Queue.Empty:
            print "Waiting for results to appear in queue"
            sleep(1)
        except Syncing:
            print "Waiting for merged results"
            sleep(1)
    print "Done getting results.  Now closing."
    tq.close()
    print "Test complete."

def exceptions_tests():
    try:
        raise PersistentQueue.Queue.NotYet
    except Exception, exc:
        assert str(type(exc)) == "<class 'PersistentQueue.Queue.NotYet'>", \
            "Failed to raise the correct exception: <class 'PersistentQueue.Queue.NotYet'> != " + str(type(exc))
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
    parser.add_option("--basic", dest="basic", default=False, action="store_true", help="run basic test")
    parser.add_option("--speed", dest="speed", default=False, action="store_true", help="run speed test")
    parser.add_option("--process", dest="process", default=False, action="store_true", help="run test of running PersistentQueue inside a multiprocessing.Process")
    parser.add_option("--lines", dest="lines", default=False, action="store_true", help="run test of PersistentQueue.LineFiles")
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
    if options.basic:
        basic_test(options.dir, options.num)
    elif options.speed:
        speed_test(options.dir, options.num)
    elif options.process:
        process_test(options.dir, options.num)
    elif options.lines:
        lines_test(options.dir, options.num)
        rmdir(options.dir)
        speed_test(options.dir, options.num, lines=True)
    elif options.sort:
        sort_test(options.dir, options.num, compress=False, compress_temps=False)
        sort_test(options.dir, options.num, compress=False, compress_temps=True)
        sort_test(options.dir, options.num, compress=True, compress_temps=False)
        sort_test(options.dir, options.num, compress=True, compress_temps=True)
    elif options.merge:
        merge_test(options.dir, options.num)
    elif options.triqueue:
        triqueue_test(options.dir, options.num)
    elif options.exceptions:
        exceptions_tests()
    else:
        basic_test(options.dir, options.num)
        rmdir(options.dir)        
        speed_test(options.dir, options.num)
        rmdir(options.dir)        
        process_test(options.dir, options.num)
        rmdir(options.dir)
        lines_test(options.dir, options.num)
        rmdir(options.dir)
        sort_test(options.dir, options.num)
        rmdir(options.dir)
        merge_test(options.dir, options.num)
        rmdir(options.dir)
        triqueue_test(options.dir, options.num)

        exceptions_tests()
        
    if not options.keep:
        rmdir(options.dir)


        

