"""
PersistentQueue provides a Queue interface to a set of flat files
stored on disk.

This is evolved (quite far) from a recipe in the public domain created
by Kjetil Jacobsen: http://code.activestate.com/recipes/501154/

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
import gzip
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
    sortable = True
    priority_field = 1
    DELIMITER = "|"

    def load(self, file):
        ret = []
        for line in file.readlines():
            line = line.strip()
            if line:
                ret.append(line)
        return ret

    def compare(self, x, y):
        """
        A function that self.dump passes into lines.sort to make the
        on-disk files sorted.
        """
        x_priority = self.get_priority(x)
        y_priority = self.get_priority(y)
        if x_priority > y_priority:
            return 1
        elif x_priority == y_priority:
            return 0
        else: # x < y
            return -1

    def dump(self, lines, file):
        lines.sort(self.compare)
        for line in lines:
            if not isinstance(line, basestring):
                line = DELIMITER.join(line)
            file.write(line + "\n")

    def make_record(self, line):
        if isinstance(line, basestring):
            return line.split(self.DELIMITER)
        else:
            return line

    def get_priority(self, line):
        rec = self.make_record(line)
        priority = float(rec[self.priority_field - 1])
        return priority        

class NotYet(Exception): pass

class PersistentQueue:
    """
    Provides a Queue interface to a set of flat files stored on disk.
    """
    def __init__(self, data_path, cache_size=512, marshal=pickle, compress=False):
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
        self.compress = compress
        self.index_file = os.path.join(data_path, INDEX_FILENAME)
        self.temp_file = os.path.join(data_path, "tempfile")
        self.data_path = os.path.join(data_path, "data")
        self.semaphore = multiprocessing.Semaphore()
        self._init_index()

    def _init_index(self):
        """
        Reconstruct index_file and in-memory caches from data on disk
        """
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)
        # if the index is there, use it
        if os.path.exists(self.index_file):
            index_file = open(self.index_file)
            self.head, self.tail = map(lambda x: int(x),
                                       index_file.read().split(" "))
            index_file.close()
        else:
            # otherwise, start from scratch
            self.head, self.tail = 0, 1
        # now setup in-memory caches
        def _load_cache(cache_name, num):
            """
            If a cache file exists on disk, load it, otherwise set the
            attr to empty list.
            """
            data_path = os.path.join(self.data_path, str(num))
            if not os.path.exists(data_path):
                setattr(self, cache_name, [])
            else:
                mode = "rb+"
                cachefile = open(data_path, mode)
                if self.compress:
                    cachefile = gzip.GzipFile(
                        mode = mode,  fileobj = cachefile, compresslevel = 9)
                try:
                    setattr(self, cache_name, self.marshal.load(cachefile))
                except EOFError:
                    setattr(self, cache_name, [])
                cachefile.close()
        # now use the function to set the two caches
        _load_cache("put_cache", self.tail)
        _load_cache("get_cache", self.head)
        assert self.head < self.tail, "Head not less than tail"

    def _sync_index(self):
        """
        Fixes the data stored in the index file to match the in-memory
        state represented by self.head and self.tail
        """
        assert self.head < self.tail, "Head not less than tail"
        index_file = open(self.temp_file, "w")
        index_file.write("%d %d" % (self.head, self.tail))
        index_file.close()
        if os.path.exists(self.index_file):
            os.remove(self.index_file)
        os.rename(self.temp_file, self.index_file)

    def _split(self):
        """
        Called whenever put_cache has grown larger than cache_size
        """
        assert len(self.put_cache) == self.cache_size, \
            "Too late: _split called after put_cache is *larger* than cache_size"
        # store put_cache in temp_file
        temp_file = open(self.temp_file, "wb")
        if self.compress:
            temp_file = gzip.GzipFile(
                mode = "wb",  fileobj = temp_file, compresslevel = 9)
        self.marshal.dump(self.put_cache, temp_file)
        temp_file.close()
        # move the temp_file to file named by tail
        put_file = os.path.join(self.data_path, str(self.tail))
        if os.path.exists(put_file):
            os.remove(put_file)
        os.rename(self.temp_file, put_file)
        # update tail, which means we must update in-memory cache
        self.tail += 1
        # put_cache is now safely on disk, so in-memory is empty:
        self.put_cache = []
        self._sync_index()

    def _join(self):
        """
        Used by get() when the in-memory get_cache is empty.

        Copes with the two possibilities:
        
           1) current position is tail, so no files on disk

           2) exist files on disk, so load next one into get_cache

        """
        # Current cache position is one higher than head.  This is the
        # only place this gets incremented:
        current = self.head + 1
        if current == self.tail:
            # no files on disk, make the get_cache the put_cache
            # (could be any length)
            self.get_cache = self.put_cache
            # the put_cache should now be empty
            self.put_cache = []
            # head and tail need not move
        else:
            # load next file from disk
            get_file = open(os.path.join(self.data_path, str(current)), "rb")
            if self.compress:
                get_file = gzip.GzipFile(
                    mode = "rb",  fileobj = get_file, compresslevel = 9)
            self.get_cache = self.marshal.load(get_file)
            get_file.close()
            # remove it
            try:
                os.remove(os.path.join(self.data_path, str(self.head)))
            except:
                pass
            # update head position: it moves one up (see above)
            self.head = current
        # make sure head is always less than tail:
        if self.head == self.tail:
            self.head = self.tail - 1
        # fix index file
        self._sync_index()

    def _sync(self):
        """
        Put the contents of both get_cache and put_cache on disk, so
        the in-memory caches are empty.
        """
        self._sync_index()
        # flush the get_cache to disk
        get_file = os.path.join(self.data_path, str(self.head))
        temp_file = open(self.temp_file, "wb")
        if self.compress:
            temp_file = gzip.GzipFile(
                mode = "wb",  fileobj = temp_file, compresslevel = 9)
        self.marshal.dump(self.get_cache, temp_file)
        temp_file.close()
        if os.path.exists(get_file):
            os.remove(get_file)
        os.rename(self.temp_file, get_file)
        # flush the put_cache to disk
        put_file = os.path.join(self.data_path, str(self.tail))
        temp_file = open(self.temp_file, "wb")
        if self.compress:
            temp_file = gzip.GzipFile(
                mode = "wb",  fileobj = temp_file, compresslevel = 9)
        self.marshal.dump(self.put_cache, temp_file)
        temp_file.close()
        if os.path.exists(put_file):
            os.remove(put_file)
        os.rename(self.temp_file, put_file)
        # as long as we do not change head or tail values, we can
        # leave the in-memory cache unchanged.

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

    def sort(self, compress_temps=False, merge_from=[]):
        """
        Break the FIFO nature of the data by sorting all the records
        on disk and putting them back into the queue in sorted order.

        compress_temps indicates whether to cause the sort function to
        compress its temporary files.  This slows it down by ~20-25%

        merge_from can be a list of other PersistentQueue instances,
        which will get emptied into this sorted queue.
        """
        if not (hasattr(self.marshal, "sortable") and self.marshal.sortable):
            return NotImplemented
        try:
            # make sure that this PersistentQueue is not in merge_from list
            merge_from.remove(self)
        except ValueError:
            # was not there
            pass
        queues = [self] + merge_from
        for pq in queues:
            assert pq.compress == self.compress, \
                "All merge_from queues must have same compress flag as this queue"
        for pq in queues:
            pq.semaphore.acquire()
        try:
            # do what close does
            for pq in queues:
                pq._sync()
                if os.path.exists(pq.temp_file):
                    try:
                        os.remove(pq.temp_file)
                    except:
                        pass
                # remove index file, so no conflict when _init_index
                os.remove(pq.index_file)
            # make a single file of all the sorted data
            sorted_path = "%s/../sorted" % self.data_path
            sorted_file = open(sorted_path, "w")
            args = ["sort", "-nu"]
            args.append("-k%d,%d" % ((self.marshal.priority_field), 
                                        self.marshal.priority_field+1))
            args.append("-t%s" % self.marshal.DELIMITER)
            if compress_temps:
                args.append("--compress-program=gzip")
            # setup the list of file paths
            files = []
            for pq in queues:
                files += [os.path.join(pq.data_path, file_name)
                          for file_name in os.listdir(pq.data_path)]
            if self.compress:
                # If we are compressing, then we must load all the
                # files here and push them over stdin, which loses
                # benefit of having sorted them when writing.
                sort = subprocess.Popen(
                    args=args,
                    stdin=subprocess.PIPE,
                    stdout=sorted_file)
                for file_path in files:
                    fh = open(file_path)
                    fh = gzip.GzipFile(
                        mode = "r",  fileobj = fh, compresslevel = 9)
                    while True:
                        line = fh.readline()
                        if not line: break
                        sort.stdin.write(line)
                    fh.close()
                sort.stdin.close()
            else:
                # If not compressing, just pass file names as args.
                # The -m means that sort can simply merge without
                # sorting, because we wrote them as sorted lists.
                args.append("-m")
                args += files
                sort = subprocess.Popen(
                    args=args,
                    stdout=sorted_file)
            syslog(LOG_DEBUG, "waiting for sort to finish, sort_file is open")
            sort.wait()
            sorted_file.close()
            syslog(LOG_DEBUG, "removing FIFO")
            for file_name in files:
                os.remove(file_name)
            syslog(LOG_DEBUG, "re-populating FIFO")
            # setup the index files and prepare for put
            for pq in queues:
                pq._init_index()
            # read in sorted_file and put into newly initialized queue
            sorted_file = open(sorted_path, "r")
            while True:
                line = sorted_file.readline()
                if not line: break
                # do what self.put() does:
                self.put_cache.append(line.strip())
                if len(self.put_cache) >= self.cache_size:
                    self._split()
            # clean up the sorted file
            sorted_file.close()
            os.remove(sorted_path)
            syslog(LOG_DEBUG, "done re-populating FIFO")
        except Exception, exc:
            multi_syslog(LOG_NOTICE, traceback.format_exc(exc))
        finally:
            for pq in queues:
                pq.semaphore.release()

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
def speed_test(data_path, ELEMENTS=50000, p=None, lines=False, compress=True):
    """run speed tests and average speeds of put and get"""
    if p is None:
        if lines:
            p = PersistentQueue(data_path, 10, LineFiles(), compress=compress)
        else:
            p = PersistentQueue(data_path, 10, compress=compress)
    start = time()
    for a in range(ELEMENTS):
        p.put(str(a))
    p.sync()
    end = time()
    elapsed = end - start 
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
        p = PersistentQueue(data_path, 10, compress=compress)
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
        p = PersistentQueue(data_path, 10, LineFiles(), compress=compress)
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

def sort_test(data_path, ELEMENTS=1000, p=None, compress=True, compress_temps=True):
    """run sort tests"""
    print "Running test on sorting with %d elements" % ELEMENTS
    import random
    if p is None:
        p = PersistentQueue(data_path, 10, LineFiles(), compress=compress)        
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
        p.put(str(a))
    # sync but do not close
    p.sync()
    # this could take time
    start = time()
    p.sort(compress_temps)
    end = time()
    elapsed = end - start
    rate = elapsed and (ELEMENTS / elapsed) or 0.0
    # get the response and compare with answer
    answer = range(ELEMENTS)
    vals = []
    for a in range(len(answer)):
        vals.append(int(p.get()))
    assert vals == answer, "Wrongly sorted result:\n%s" % vals
    for i in range(len(vals)-1):
        assert vals[i] <= vals[i+1], "Wrongly sorted result:\n%s" % vals
    print "Sorting succeeded.  Sort took %s seconds, %.2f records/second" \
        % (elapsed, rate)
    p.close()

def merge_test(data_path, ELEMENTS=1000):
    """run sort tests"""
    num_queues = 4
    print "Running test on merging with %d elements from %d queues" \
        % (ELEMENTS, num_queues)
    import random
    p = PersistentQueue(data_path, 10, LineFiles())
    queues = [p]
    for i in range(num_queues - 1):
        queues.append(
            PersistentQueue(data_path + "/%d" % i, 10, LineFiles()))
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
        pq.put(str(a))
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
        vals.append(int(p.get()))
    assert vals == answer, "Wrongly sorted result:\n%s" % vals
    for i in range(len(vals)-1):
        assert vals[i] <= vals[i+1], "Wrongly sorted result:\n%s" % vals
    print "Sorting succeeded.  Sort took %s seconds, %.2f records/second" \
        % (elapsed, rate)
    p.close()
    for pq in queues:
        assert len(pq) == 0, "Should not have any items left in merge_from queues"
        pq.close()

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
        self.queue = PersistentQueue(self.data_path, compress=True)
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
    speed_test(data_path, ELEMENTS, p=pqc.queue)
    basic_test(data_path, ELEMENTS, p=pqc.queue)
    pqc.close()

def rmdir(dir):
    if os.path.exists(dir):
        try:
            shutil.rmtree(dir)
        except Exception, exc:
            print "Did not rmtree the dir. " + str(exc)

if __name__ == "__main__":
    import shutil
    from optparse import OptionParser
    parser = OptionParser(description="runs tests for PersistentQueue.  Default runs all tests.")
    parser.add_option("-n", "--num", dest="num", default=1000, type=int, help="num items to put/get in tests")
    parser.add_option("--dir", dest="dir", default="PersistentQueue_testdir", help="path for dir to use in tests")
    parser.add_option("--basic", dest="basic", default=False, action="store_true", help="run basic test")
    parser.add_option("--speed", dest="speed", default=False, action="store_true", help="run speed test")
    parser.add_option("--process", dest="process", default=False, action="store_true", help="run test of running PersistentQueue inside a multiprocessing.Process")
    parser.add_option("--lines", dest="lines", default=False, action="store_true", help="run test of LineFiles")
    parser.add_option("--sort", dest="sort", default=False, action="store_true", help="run test of sorting")
    parser.add_option("--merge", dest="merge", default=False, action="store_true", help="run test of sorted merging of multiple queues")
    parser.add_option("--keep", dest="keep", default=False, action="store_true", help="keep the data dir after the test")
    (options, args) = parser.parse_args()
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

    if not options.keep:
        rmdir(options.dir)

