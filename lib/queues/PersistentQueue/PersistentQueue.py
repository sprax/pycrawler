#!/usr/bin/python2.6
"""
PersistentQueue.Queue provides a queue interface to a set of flat
files stored on disk.

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
import copy
import gzip
import fcntl
import Queue as NormalQueue
import cPickle as pickle
#import marshal
import traceback
import subprocess
from time import time
from syslog import syslog, LOG_DEBUG, LOG_NOTICE

class Mutex(object):
    """
    Class wrapping around a file that is used as a mutex.
    """
    def __init__(self, lock_path, 
                 acquire_callback=None, release_callback=None):
        """
        If lock_path exists, then this simply points to it without
        attempting to acquire a lock.

        If lock_path does not exist, this creates it and leaves it
        unlocked.

        If defined, the acquire_callback is a function that take no
        arguments and gets called by acquire() after acquiring the
        file lock.  Similarly for release_callback, except it is
        called before releasing the file lock.
        """
        self._lock_path = lock_path
        self._acquire_callback = acquire_callback
        self._release_callback = release_callback
        self._fh = None
        if os.path.exists(lock_path):
            assert os.path.isfile(lock_path), \
                "lock_path must be a regular file"
        else:
            parent = os.path.dirname(lock_path)
            if not os.path.exists(parent):
                try:
                    os.makedirs(parent)
                except OSError, exc:
                    # okay if other process has just created it
                    if exc.errno == 17:
                        pass
                    else:
                        raise
            # create the file, but do not block, in case another
            # process is doing this.
            self.acquire(False)
            self.release()

    def acquire(self, block=True):
        """
        Open the file handle, and get an exclusive lock.  Returns True
        when acquired.

        If 'block' is False, then return False if the lock was not
        acquired.  Otherwise, returns True.
        """
        fd = os.open(self._lock_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND)
        self._fh = os.fdopen(fd, "a")
        lock_flags = fcntl.LOCK_EX
        if not block:
            lock_flags |= fcntl.LOCK_NB
        try:
            fcntl.flock(self._fh, lock_flags)
        except IOError, e:
            if e[0] == 11:
                return False
            raise
        if self._acquire_callback is not None:
            self._acquire_callback()
        return True

    def release(self):
        """
        Releases lock and closes file handle.  Can be called multiple
        times and will behave as though called only once.
        """
        if self._fh is not None:
            if self._release_callback is not None:
                self._release_callback()
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            self._fh.close()
            self._fh = None

    def available(self):
        "returns bool whether mutex is not locked"
        acquired = self.acquire(block=False)
        if acquired:
            self.release()
            return True
        else:
            return False

class FakeMutex:
    """
    Has acquired and release, but they do nothing.
    """
    def acquire(self):
        return
    def release(self):
        return

class Writer:
    """
    a Writer instance wraps a PersistentQueue, and holds it in a
    locked state, so that the caller can write already serialized data
    directly into the queue's flat files.  This relies on the caller
    to have:

        * closed the queue

        * acquired the mutex, which can be passed into this
          function to release whenever writer.close() is called.
          If the mutex is passed in, then calling
          writer.close() will also call _open on the queue before
          releasing the mutex.

        * computed 'count', which is the current number of objects
          already serialized into the file pointed to by queue._tail
    """
    def __init__(self, queue, count, mutex=None):
        """
        Initialize a file-like object for writing directly to the
        files hidden inside a PersistentQueue (specified by 'queue')

        'mutex' is passed separately from queue, so that the caller
        can control whether close() releases the mutex.
        """
        self._queue = queue
        self._count = count
        self._mutex = mutex
        self._current = None
        self._open(replace=False)

    def _open(self, replace):
        """
        opens self.current file based on self.queue.tail.  If
        'replace' is True, then any existing file is overwritten.  If
        'replace' is False, then new records are appended to the end.
        """
        if replace:
            mode = "wb"
        else:
            mode = "ab"
        file_path = os.path.join(
            self._queue._data_path, 
            str(self._queue._tail))
        self._current = open(file_path, mode)
        if self._queue._compress:
            self._current = gzip.GzipFile(
                mode = mode,  fileobj = self._current, compresslevel = 9)

    def _split(self):
        """
        Called by 'write' whenever the cache size is reached
        """
        self._queue._tail += 1
        self._count = 0
        self._current.close()
        self._open(replace=True)

    def writeline(self, line):
        """
        Check if current file is full, _split if necessary,
        and append record to end of current file.
        """
        if self._count == self._queue._cache_size:
            self._split()
        # ensure that line ends in a newline
        if line[-1] != "\n":
            line += "\n"
        self._current.write(line)
        self._count += 1

    def close(self):
        """
        Closes the current file and releases the mutex.
        """
        # we have updated queue.tail, so fix on-disk index
        self._queue._sync_index()
        self._current.close()
        if self._mutex is not None:
            self._mutex.release()
# end of Writer class

class PersistentQueue:

    class NotYet(Exception): pass

    """
    Provides a Queue interface to a set of flat files stored on disk.
    """
    def __init__(self, data_path, cache_size=512, marshal=pickle, compress=False, singleprocess=False):
        """
        Create a persistent FIFO queue named by the 'data_path' argument.

        The number of cached queue items at the head and tail of the queue
        is determined by the optional 'cache_size' parameter.  By default
        the marshal module is used to (de)serialize queue items, but you
        may specify an alternative serialize module/instance with the
        optional 'marshal' argument (e.g. pickle).

        'unlocked' indicates whether this Queue can operate with or
        without a mutex.  When 'unlocked' is True, then method calls
        will not attempt to acquire the file-based mutex.

        'compress' indicates whether or not to gzip the flat files.
        """
        assert cache_size > 0, "Cache size must be larger than 0"
        # assign basic config properties
        self._cache_size = cache_size
        self._marshal = marshal
        self._compress = compress
        # compute index and tempfile paths
        self._index_file = os.path.join(data_path, "index")
        self._temp_path = os.path.join(data_path, "tempfile")
        # compute data directory path, and create it
        self._data_path = os.path.join(data_path, "data")
        if not os.path.exists(self._data_path):
            try:
                os.makedirs(self._data_path)
            except OSError, exc:
                # okay if someone else just made it:
                if exc.errno == 17:
                    pass
                else:
                    raise
        # setup in-memory state
        self._head = None
        self._tail = None
        self._put_cache = None
        self._get_cache = None
        if singleprocess:
            self._singleprocess = True
            # no need for a mutex, so use a fake one
            self._mutex_path = None
            self._mutex = FakeMutex()
            # populate in-memory state once for all time
            self._open()
        else:
            self._singleprocess = False
            self._mutex_path = os.path.join(data_path, "lock_file")
            self._mutex = Mutex(
                self._mutex_path,
                acquire_callback=lambda: self._open(),
                release_callback=lambda: self._close())
            # make sure nobody else is trying to open right now, and
            # then open for this instance.
            self._mutex.acquire()  # this calls _open as a callback
            self._mutex.release()  # this calls _close as a callback

    def only_singleprocess(self):
        """
        returns bool whether this was opened for use with only a
        singleprocess
        """
        return self._singleprocess

    def _open(self):
        """
        Reconstruct in-memory caches from data on disk.  Can be called
        multiple times, but sync must be called after any data is
        added and before _open is called.  The close method also calls
        sync.
        """
        # start in-memory pointers from scratch
        self._head, self._tail = 0, 1
        # if the index is there, reset them
        if os.path.exists(self._index_file):
            try:
                index_file = open(self._index_file)
                index_data = index_file.read()
                index_file.close()
                # last step in try/except sets the pointers
                self._head, self._tail = \
                    map(lambda x: int(x), index_data.split(" "))
            except Exception, exc:
                # send full traceback to syslog in readable form
                map(lambda line: syslog(LOG_NOTICE, line), 
                    traceback.format_exc(exc).splitlines())
        # now setup in-memory caches
        def _load_cache(cache_name, num):
            """
            If a cache file exists on disk, load it, otherwise set the
            attr to empty list.
            """
            data_path = os.path.join(self._data_path, str(num))
            if not os.path.exists(data_path):
                setattr(self, cache_name, [])
            else:
                mode = "rb+"
                cachefile = open(data_path, mode)
                if self._compress:
                    cachefile = gzip.GzipFile(
                        mode = mode,  fileobj = cachefile, compresslevel = 9)
                try:
                    setattr(self, cache_name, self._marshal.load(cachefile))
                except EOFError:
                    setattr(self, cache_name, [])
                cachefile.close()
        # now use the function to set the two caches
        _load_cache("_put_cache", self._tail)
        _load_cache("_get_cache", self._head)
        assert self._head < self._tail, "Head not less than tail"

    def _sync_index(self):
        """
        Fixes the data stored in the index file to match the in-memory
        state represented by self._head and self._tail
        """
        assert self._head < self._tail, "Head not less than tail"
        index_file = open(self._temp_path, "w")
        index_file.write("%d %d" % (self._head, self._tail))
        index_file.close()
        if os.path.exists(self._index_file):
            os.remove(self._index_file)
        os.rename(self._temp_path, self._index_file)

    def _write_cache(self, cache, rel_data_path):
        """
        Writes the contents of 'cache' (a list) into a file at
        data_path/rel_data_path using self._marshal
        """
        # store cache in temp_file
        temp_file = open(self._temp_path, "wb")
        if self._compress:
            temp_file = gzip.GzipFile(
                mode = "wb",  fileobj = temp_file, compresslevel = 9)
        self._marshal.dump(cache, temp_file)
        temp_file.close()
        # move the temp_file to file named by tail
        if not isinstance(rel_data_path, basestring):
            rel_data_path = str(rel_data_path)
        file_path = os.path.join(self._data_path, rel_data_path)
        if os.path.exists(file_path):
            os.remove(file_path)
        os.rename(self._temp_path, file_path)

    def _split(self):
        """
        Called whenever put_cache has grown larger than cache_size
        """
        assert len(self._put_cache) == self._cache_size, \
            "Too late: _split called after put_cache is *larger* than cache_size"
        self._write_cache(self._put_cache, self._tail)
        # update tail, which means we must update in-memory cache
        self._tail += 1
        # put_cache is now safely on disk, so in-memory is empty:
        self._put_cache = []
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
        current = self._head + 1
        if current == self._tail:
            # no files on disk, make the get_cache the put_cache
            # (could be any length)
            self._get_cache = self._put_cache
            # the put_cache should now be empty
            self._put_cache = []
            # head and tail need not move
        else:
            # load next file from disk
            get_file = open(os.path.join(self._data_path, str(current)), "rb")
            if self._compress:
                get_file = gzip.GzipFile(
                    mode = "rb",  fileobj = get_file, compresslevel = 9)
            self._get_cache = self._marshal.load(get_file)
            get_file.close()
            # remove it
            try:
                os.remove(os.path.join(self._data_path, str(self._head)))
            except:
                pass
            # update head position: it moves one up (see above)
            self._head = current
        # make sure head is always less than tail:
        if self._head == self._tail:
            self._head = self._tail - 1
        # fix index file
        self._sync_index()

    def _sync(self):
        """
        Put the contents of both get_cache and put_cache on disk
        """
        if self._get_cache is None: 
            # queue is closed
            return
        self._sync_index()
        self._write_cache(self._get_cache, self._head)
        self._write_cache(self._put_cache, self._tail)
        # as long as we do not change head or tail values, we can
        # leave the in-memory cache unchanged.

    def __len__(self):
        """
        Return number of items in queue.
        """
        self._mutex.acquire()
        try:
            if self._get_cache is None:
                return (self._tail - self._head - 1) * self._cache_size
            return ((self._tail - self._head - 1) * self._cache_size) + \
                    len(self._put_cache) + len(self._get_cache)
        finally:
            self._mutex.release()

    def sort(self, 
             compress_temps=False, 
             merge_from=[], merge_to=None,
             unique=False,
             numerical=True):
        """
        Break the FIFO nature of the data by sorting all the records
        on disk and putting them back into the queue in sorted order.

        compress_temps indicates whether to cause the sort function to
        compress its temporary files.  This slows it down by ~20-25%

        merge_from can be a list of other PersistentQueue instances,
        which will get emptied into this sorted queue.

        merge_to can be a different PersistentQueue instance into
        which to put all records (instead of into this queue).
        merge_to can be in the list of merge_from.

        If self._marshal has an 'accumulator' method, then it will be
        applied to every item passing through the sort.  Whenever the
        second return value is not None, then it is written to the
        newly merged queue.
        """
        if not (hasattr(self._marshal, "_sort_key") \
                    and self._marshal._sort_key is not None):
            return NotImplemented
        if self in merge_from:
            queues = merge_from
        else:
            queues = [self] + merge_from
        for pq in queues:
            assert pq._compress == self._compress, \
                "All merge_from queues must have same compress flag as this queue"
        for pq in queues:
            pq._mutex.acquire()
        try:
            # do what close does
            for pq in queues:
                pq._sync()
                if os.path.exists(pq._temp_path):
                    try:
                        os.remove(pq._temp_path)
                    except:
                        pass
                # remove index file, so no conflict when _open
                os.remove(pq._index_file)
            # make a single file of all the sorted data
            sorted_path = "%s/../sorted" % self._data_path
            sorted_file = open(sorted_path, "w")
            args = ["sort"]
            # treat sort field as a number
            if numerical:
                args.append("-n")
            # keep only first of multiple records with same sort field
            if unique:  
                args.append("-u")
            # define the sort field
            args.append(
                "-k%d,%d" % (
                    self._marshal._sort_key + 1, 
                    self._marshal._sort_key + 2))
            # define field separator
            args.append("-t%s" % self._marshal.DELIMITER)
            if compress_temps:
                args.append("--compress-program=gzip")
            # setup the list of file paths
            files = []
            for pq in queues:
                files += [os.path.join(pq._data_path, file_name)
                          for file_name in os.listdir(pq._data_path)]
            #for file_name in files:
            #    print "%s --> \n\t%s" % (
            #        file_name, 
            #        "\n\t".join(open(file_name).read().splitlines()))
            if self._compress:
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
                    # end the last line before writing from next file
                    sort.stdin.write("\n")
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
            #syslog(LOG_DEBUG, "waiting for sort to finish, sort_file is open")
            sort.wait()
            sorted_file.close()
            #print " ".join(args)
            #print "sorted_path has %d" % len(open(sorted_path).read().splitlines())
            #print "%s --> \n\t%s" % (
            #    sorted_path, 
            #    "\n\t".join(open(sorted_path).read().splitlines()))
            #sys.exit()
            # remove all files from all queues
            for file_name in files:
                os.remove(file_name)
            # fix in-memory head/tail state
            for pq in queues:
                pq._head = 0
                pq._tail = 1
            if merge_to in merge_from:
                # we acquired mutex, and count is zero
                writer = Writer(merge_to, 0)
            elif merge_to is not None:
                # have it acquire mutex and compute count
                writer = merge_to.get_writer()
            else:
                # we acquired mutex, and count is zero
                writer = Writer(self, 0)
            # read in sorted_file and writelines with Writer
            sorted_file = open(sorted_path, "r")
            #c = 0
            if hasattr(self._marshal, "accumulator") and \
                    self._marshal.accumulator is not None:
                accumulator = lambda x, y: self._marshal.accumulator(x, y)
            else:
                accumulator = None
            acc_state = None
            while True:
                line = sorted_file.readline()
                if accumulator is None:
                    if not line: break
                    #if not line.strip(): continue
                    writer.writeline(line)
                else:
                    # see description of accumulators in nameddict
                    acc_state, line = accumulator(acc_state, line)
                    if line is not None:
                        writer.writeline(line)
                    if acc_state is None:
                        break
                #c += 1
            #print "called writeline %d times" % c
            writer.close()
            # clean up the sorted file
            sorted_file.close()
            os.remove(sorted_path)
            return True
        finally:
            # setup the index files and prepare usage
            for pq in queues:
                pq._open()
            for pq in queues:
                pq._mutex.release()

    def get_writer(self):
        """
        Returns a file-like object that has 'writeline' and 'close'
        methods for allowing the caller to write directly to disk
        records that have already been serialized.
        """
        self._mutex.acquire()
        # in-memory put_cache has same number of records as the
        # on-disk file identified by self._tail
        count = len(self._put_cache)
        # flush to disk and close in-memory data structures, so we can
        # call _open in Writer.close()
        self._close()
        return Writer(self, count, self._mutex)

    def sync(self):
        """
        Synchronize memory caches to disk.
        """
        self._mutex.acquire()
        try:
            self._sync()
        finally:
            self._mutex.release()

    def put(self, nd):
        """
        Put a copy of the nameddict subclass 'nd' on the queue.
        """
        #syslog("%s doing a put of: %s" % (self._data_path, nd))
        self._mutex.acquire()
        try:
            nd = copy.copy(nd)
            self._put_cache.append(nd)
            if len(self._put_cache) >= self._cache_size:
                self._split()
        finally:
            self._mutex.release()

    def get(self, maxP=None):
        """
        Get an item from the queue with the (optional) constraint that
        the priority field (a float) is less than or equal to maxP.
        The marshal tool must provide a get_priority method.

        Throws Queue.Empty exception if the queue is empty.

        Throws NotYet exception if the queue is not empty but the next
        record's priority is greater than maxP.
        """
        self._mutex.acquire()
        #start = time()
        try:
            if len(self._get_cache) == 0:
                # load next cache file
                self._join()
            # in-memory cache is fresh, so if empty:
            if len(self._get_cache) == 0:
                raise NormalQueue.Empty
            # not empty, so consider next record
            rec = self._get_cache.pop(0)
            if maxP is None or \
                    self._marshal.get_sort_val(rec) <= maxP:
                return rec
            else:
                # rejecting it because of maxP priority
                self._get_cache.insert(0, rec)
                syslog("not yet: " + str(rec))
                raise self.NotYet
        finally:
            #end = time()
            #print "%.3f seconds to do internal getting" % (end - start)
            self._mutex.release()

    def _close(self):
        """
        Sync the caches and remove any temp_path
        """
        self._sync()
        if os.path.exists(self._temp_path):
            try:
                os.remove(self._temp_path)
            except:
                pass
        self._put_cache = None
        self._get_cache = None

    def close(self):
        """
        Close the queue.  Implicitly synchronizes memory caches to disk.
        No further accesses should be made through this queue instance.
        """
        self._mutex.acquire()
        try:
            self._close()
        finally:
            self._mutex.release()

    def transfer_to(self, other_q):
        """
        Moves all data out of this queue and into another instance of
        PersistentQueue.  This implementation just acquires the
        mutex and calls get until there are no more records.  A
        faster implementation would move the actual files over to the
        other_q.  A better implementation might also relinquish the
        mutex earlier by changing the index before doing anything
        with the data.
        """
        # prevent race condition where others add more to the queue
        # and never let us finish.
        self._mutex.acquire()
        while True:
            try:
                if len(self._get_cache) > 0:
                    rec = self._get_cache.pop(0)
                else:
                    self._join()
                    if len(self._get_cache) > 0:
                        rec = self._get_cache.pop(0)
                    else:
                        raise NormalQueue.Empty
                other_q.put(rec)
            except NormalQueue.Empty:
                break
        self._mutex.release()
