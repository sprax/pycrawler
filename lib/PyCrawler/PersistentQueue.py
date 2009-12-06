"""
PersistentQueue provides a Queue interface to a set of flat files
stored on disk.

This is evolved from a recipe in the public domain created by Kjetil
Jacobsen: http://code.activestate.com/recipes/501154/

"""
# $Id$
__copyright__ = "Copyright 2009, John R. Frank"
__credits__ = ["Kjetil Jacobsen"]
__license__ = "MIT License"
__version__ = "0.1"
__maintainer__ = "John R. Frank"
import os
import sys
import copy
import Queue
import cPickle as pickle
#import marshal
import traceback
import subprocess
import multiprocessing
from time import time, sleep
from syslog import syslog, LOG_INFO, LOG_DEBUG, LOG_NOTICE
from Process import Process, multi_syslog

# Filename used for index files, must not contain numbers
INDEX_FILENAME = "index"

class LineFiles:
    DELIMITER = "|"
    def load(self, file):
        ret = []
        for line in file.readlines():
            line = line.strip()
            if line:
                ret.append(line)
        return ret
    def dump(self, lines, file):
        file.write("\n".join(lines) + "\n")

    def make_record(self, line):
        return line.split(self.DELIMITER)

    def get_priority(self, line):
        rec = self.make_record(line)
        priority = float(rec[0])
        return priority        

class NotYet(Exception): pass

class PersistentQueue:
    """
    Provides a Queue interface to a set of flat files stored on disk.
    """
    def __init__(self, data_path, cache_size=512, marshal=pickle):
        """
        Create a persistent FIFO queue named by the 'data_path' argument.

        The number of cached queue items at the head and tail of the queue
        is determined by the optional 'cache_size' parameter.  By default
        the marshal module is used to (de)serialize queue items, but you
        may specify an alternative serialize module/instance with the
        optional 'marshal' argument (e.g. pickle).
        """
        assert cache_size > 0, "Cache size must be larger than 0"
        self.cache_size = cache_size
        self.marshal = marshal
        self.index_file = os.path.join(data_path, INDEX_FILENAME)
        self.temp_file = os.path.join(data_path, "tempfile")
        self.data_path = os.path.join(data_path, "data")
        self.semaphore = multiprocessing.Semaphore()
        self._init_index()

        self.get_nowait = self.get
        self.put_nowait = self.put

    def _init_index(self):
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)
        if os.path.exists(self.index_file):
            index_file = open(self.index_file)
            self.head, self.tail = map(lambda x: int(x),
                                       index_file.read().split(" "))
            index_file.close()
        else:
            self.head, self.tail = 0, 1
        def _load_cache(cache, num):
            data_path = os.path.join(self.data_path, str(num))
            mode = "rb+" if os.path.exists(data_path) else "wb+"
            cachefile = open(data_path, mode)
            try:
                setattr(self, cache, self.marshal.load(cachefile))
            except EOFError:
                setattr(self, cache, [])
            cachefile.close()
        _load_cache("put_cache", self.tail)
        _load_cache("get_cache", self.head)
        assert self.head < self.tail, "Head not less than tail"

    def _sync_index(self):
        assert self.head < self.tail, "Head not less than tail"
        index_file = open(self.temp_file, "w")
        index_file.write("%d %d" % (self.head, self.tail))
        index_file.close()
        if os.path.exists(self.index_file):
            os.remove(self.index_file)
        os.rename(self.temp_file, self.index_file)

    def _split(self):
        put_file = os.path.join(self.data_path, str(self.tail))
        temp_file = open(self.temp_file, "wb")
        self.marshal.dump(self.put_cache, temp_file)
        temp_file.close()
        if os.path.exists(put_file):
            os.remove(put_file)
        os.rename(self.temp_file, put_file)
        self.tail += 1
        if len(self.put_cache) <= self.cache_size:
            self.put_cache = []
        else:
            self.put_cache = self.put_cache[:self.cache_size]
        self._sync_index()

    def _join(self):
        current = self.head + 1
        if current == self.tail:
            self.get_cache = self.put_cache
            self.put_cache = []
        else:
            get_file = open(os.path.join(self.data_path, str(current)), "rb")
            self.get_cache = self.marshal.load(get_file)
            get_file.close()
            try:
                os.remove(os.path.join(self.data_path, str(self.head)))
            except:
                pass
            self.head = current
        if self.head == self.tail:
            self.head = self.tail - 1
        self._sync_index()

    def _sync(self):
        self._sync_index()
        get_file = os.path.join(self.data_path, str(self.head))
        temp_file = open(self.temp_file, "wb")
        self.marshal.dump(self.get_cache, temp_file)
        temp_file.close()
        if os.path.exists(get_file):
            os.remove(get_file)
        os.rename(self.temp_file, get_file)
        put_file = os.path.join(self.data_path, str(self.tail))
        temp_file = open(self.temp_file, "wb")
        self.marshal.dump(self.put_cache, temp_file)
        temp_file.close()
        if os.path.exists(put_file):
            os.remove(put_file)
        os.rename(self.temp_file, put_file)

    def __len__(self):
        """
        Return number of items in queue.
        """
        self.semaphore.acquire()
        try:
            return (((self.tail-self.head)-1)*self.cache_size) + \
                    len(self.put_cache) + len(self.get_cache)
        finally:
            self.semaphore.release()

    def sort(self):
        """
        Break the FIFO nature of the data by sorting all the records
        on disk
        """
        self.semaphore.acquire()
        try:
            self._sync()
            files = os.listdir(self.data_path)
            if not files: return
            sorted_path = "%s/../sorted" % self.data_path
            sorted_file = open(sorted_path, "w")
            sort = subprocess.Popen(
                ["sort", "-u", "-"],
                stdin=subprocess.PIPE,
                stdout=sorted_file)
            for chunk_name in files:
                chunk = open(os.path.join(self.data_path, chunk_name))
                while True:
                    line = chunk.readline()
                    if not line: break
                    line = line.strip()
                    sort.communicate(line)
            sort.stdin.close()
            syslog(LOG_DEBUG, "waiting for sort to finish")
            sort.wait()
            sorted_file.close()
            syslog(LOG_DEBUG, "removing FIFO")
            for chunk_name in files:
                os.remove(os.path.join(self.data_path, chunk_name))
            syslog(LOG_DEBUG, "re-populating FIFO")
            sorted_file = open(sorted_path, "r")
            for line in sorted_file.readlines():
                line = line.strip()
                if not line: continue
                self.put_cache.append(line)
                if len(self.put_cache) >= self.cache_size:
                    self._split()
            sorted_file.close()
            os.remove(sorted_path)
            syslog(LOG_DEBUG, "done re-populating FIFO")
        except Exception, exc:
            multi_syslog(LOG_NOTICE, traceback.format_exc(exc))
        finally:
            self.semaphore.release()

    def sync(self):
        """
        Synchronize memory caches to disk.
        """
        self.semaphore.acquire()
        try:
            self._sync()
        finally:
            self.semaphore.release()

    def put(self, obj):
        """
        Put the item 'obj' on the queue.
        """
        self.semaphore.acquire()
        try:
            self.put_cache.append(copy.copy(obj))
            if len(self.put_cache) >= self.cache_size:
                self._split()
        finally:
            self.semaphore.release()
            #self.sync()

    def get(self):
        """
        Get an item from the queue.
        Throws Empty exception if the queue is empty.
        """
        self.semaphore.acquire()
        try:
            if len(self.get_cache) > 0:
                return self.get_cache.pop(0)
            else:
                self._join()
                if len(self.get_cache) > 0:
                    return self.get_cache.pop(0)
                else:
                    raise Queue.Empty
        finally:
            self.semaphore.release()

    def getif(self, maxP=0):
        """
        Get an item from the queue with the constraint that the first
        field (a float) is less than or equal to maxP.

        Throws Empty exception if the queue is empty.

        Throws NotYet exception if the queue is not empty but the next
        record's first field is greater than maxP.
        """
        self.semaphore.acquire()
        try:
            if len(self.get_cache) > 0:
                line = self.get_cache.pop(0)
                if self.marshal.get_priority(line) <= maxP:
                    return line
                else:
                    self.get_cache.insert(0, line)
                    raise NotYet
            else:
                self._join()
                if len(self.get_cache) > 0:
                    linie = self.get_cache.pop(0)
                    if self.marshal.get_priority(line) <= maxP:
                        return line
                    else:
                        self.get_cache.insert(0, line)
                        raise NotYet
                else:
                    raise Queue.Empty
        finally:
            self.semaphore.release()

    def close(self):
        """
        Close the queue.  Implicitly synchronizes memory caches to disk.
        No further accesses should be made through this queue instance.
        """
        self.semaphore.acquire()
        try:
            self._sync()
            if os.path.exists(self.temp_file):
                try:
                    os.remove(self.temp_file)
                except:
                    pass
        finally:
            self.semaphore.release()

    def transfer_to(self, other_q):
        """
        Moves all data out of this queue and into another instance of
        PersistentQueue.  This implementation just acquires the
        semaphore and calls get until there are no more records.  A
        faster implementation would move the actual files over to the
        other_q.  A better implementation might also relinquish the
        semaphore earlier by changing the index before doing anything
        with the data.
        """
        # prevent race condition where others add more to the queue
        # and never let us finish.
        self.semaphore.acquire()
        while True:
            try:
                if len(self.get_cache) > 0:
                    rec = self.get_cache.pop(0)
                else:
                    self._join()
                    if len(self.get_cache) > 0:
                        rec = self.get_cache.pop(0)
                    else:
                        raise Queue.Empty
                other_q.put(rec)
            except Queue.Empty:
                break
        self.semaphore.release()
            
## Tests
def speed_test(data_path, ELEMENTS=50000, p=None):
    """run speed tests and average speeds of put and get"""
    if p is None:
        p = PersistentQueue("test", 10)
    p = PersistentQueue("test", 10)
    start = time()
    for a in range(ELEMENTS):
        p.put(str(a))
    p.sync()
    end = time()
    elapsed = end - start 
    print "put --> (%d rec / %.3f sec) = %.3f rec/sec" % (ELEMENTS, elapsed, ELEMENTS/elapsed)
    start = time()
    for a in range(ELEMENTS):
        p.get()
    end = time()
    elapsed = end - start 
    print "get() --> (%d rec / %.3f sec) = %.3f rec/sec" % (ELEMENTS, elapsed, ELEMENTS/elapsed)
    p.sync()
    p.close()

def basic_test(data_path, ELEMENTS=1000, p=None):
    """run basic tests"""
    if p is None:
        p = PersistentQueue("test", 10)
    print "Enqueueing %d items, cache size = %d" % (ELEMENTS,
                                                    p.cache_size)
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

class PersistentQueueContainer(Process):
    def __init__(self, id, go, data_path):
        self.name = id
        Process.__init__(self)
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
        """
        syslog("Starting")
        self.queue = PersistentQueue(self.data_path)
        while self.go.is_set() and self._go.is_set():
            sleep(1)
        syslog("syncing before closing")
        self.queue.sync()
        self.queue.close()
        syslog("Done.")
        self.stop()

def process_test(data_path, ELEMENTS):
    pqc = PersistentQueueContainer(data_path, None, data_path)
    pqc.start()
    sleep(1)
    speed_test(ELEMENTS, p=pqc.queue)
    basic_test(ELEMENTS, p=pqc.queue)
    pqc.close()

if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser(description="runs tests for PersistentQueue.  Default runs all tests.")
    parser.add_option("-n", "--num", dest="num", default=1000, type=int, help="num items to put/get in tests")
    parser.add_option("--dir", dest="dir", default="PersistentQueue_testdir", help="path for dir to use in tests")
    parser.add_option("--basic", dest="basic", default=False, action="store_true", help="run basic test")
    parser.add_option("--speed", dest="speed", default=False, action="store_true", help="run speed test")
    parser.add_option("--process", dest="process", default=False, action="store_true", help="run test of running PersistentQueue inside a multiprocessing.Process")
    (options, args) = parser.parse_args()
    if options.basic:
        basic_test(options.dir, options.num)
    elif options.speed:
        speed_test(options.dir, options.num)
    elif options.process:
        process_test(options.dir, options.num)
    else:
        basic_test(options.dir, options.num)
        speed_test(options.dir, options.num)
        process_test(options.dir, options.num)
