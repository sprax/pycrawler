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
        self.temp_path = os.path.join(data_path, "tempfile")
        self.data_path = os.path.join(data_path, "data")
        self.semaphore = multiprocessing.Semaphore()
        self.head = None
        self.tail = None
        self.put_cache = None
        self.get_cache = None
        # set by make_multiprocess_safe and make_singleprocess_safe
        self._multiprocessing = False
        self._open()

    def _open(self):
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
        index_file = open(self.temp_path, "w")
        index_file.write("%d %d" % (self.head, self.tail))
        index_file.close()
        if os.path.exists(self.index_file):
            os.remove(self.index_file)
        os.rename(self.temp_path, self.index_file)

    def _write_cache(self, cache, rel_data_path):
        """
        Writes the contents of 'cache' (a list) into a file at
        data_path/rel_data_path using self.marshal
        """
        # store cache in temp_file
        temp_file = open(self.temp_path, "wb")
        if self.compress:
            temp_file = gzip.GzipFile(
                mode = "wb",  fileobj = temp_file, compresslevel = 9)
        self.marshal.dump(cache, temp_file)
        temp_file.close()
        # move the temp_file to file named by tail
        if not isinstance(rel_data_path, basestring):
            rel_data_path = str(rel_data_path)
        file_path = os.path.join(self.data_path, rel_data_path)
        if os.path.exists(file_path):
            os.remove(file_path)
        os.rename(self.temp_path, file_path)

    def _split(self):
        """
        Called whenever put_cache has grown larger than cache_size
        """
        assert len(self.put_cache) == self.cache_size, \
            "Too late: _split called after put_cache is *larger* than cache_size"
        self._write_cache(self.put_cache, self.tail)
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
        Put the contents of both get_cache and put_cache on disk
        """
        if self.get_cache is None: 
            # queue is closed
            return
        self._sync_index()
        self._write_cache(self.get_cache, self.head)
        self._write_cache(self.put_cache, self.tail)
        # as long as we do not change head or tail values, we can
        # leave the in-memory cache unchanged.

    def __len__(self):
        """
        Return number of items in queue.
        """
        self.semaphore.acquire()
        try:
            if self._multiprocessing:
                self._open()
            return ((self.tail - self.head - 1) * self.cache_size) + \
                    len(self.put_cache) + len(self.get_cache)
        finally:
            if self._multiprocessing:
                self._close()
            self.semaphore.release()

    def sort(self, compress_temps=False, merge_from=[], merge_to=None):
        """
        Break the FIFO nature of the data by sorting all the records
        on disk and putting them back into the queue in sorted order.

        compress_temps indicates whether to cause the sort function to
        compress its temporary files.  This slows it down by ~20-25%

        merge_from can be a list of other PersistentQueue instances,
        which will get emptied into this sorted queue.

        merge_to can be a different PersistentQueue instance into
        which to put all records (instead of into this queue).

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
                if os.path.exists(pq.temp_path):
                    try:
                        os.remove(pq.temp_path)
                    except:
                        pass
                # remove index file, so no conflict when _open
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
            for file_name in files:
                os.remove(file_name)
            syslog(LOG_DEBUG, "re-populating FIFO")
            # setup the index files and prepare for put
            for pq in queues:
                pq._open()
            # read in sorted_file and put into newly initialized queue
            sorted_file = open(sorted_path, "r")
            while True:
                line = sorted_file.readline()
                if not line: break
                line = line.strip()
                if merge_to is not None:
                    merge_to.put(line)
                else:
                    # do what self.put() does:
                    self.put_cache.append(line)
                    if len(self.put_cache) >= self.cache_size:
                        self._split()
            # cause _sync:
            if merge_to is not None:
                merge_to.sync()
            else:
                self._sync()
            # clean up the sorted file
            sorted_file.close()
            os.remove(sorted_path)
            return True
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
        #syslog("%s doing a put of: %s" % (self.data_path, obj))
        self.semaphore.acquire()
        try:
            if self._multiprocessing:
                self._open()
            self.put_cache.append(copy.copy(obj))
            if len(self.put_cache) >= self.cache_size:
                self._split()
        finally:
            if self._multiprocessing:
                self._close()
            self.semaphore.release()

    def get(self, maxP=None):
        """
        Get an item from the queue with the (optional) constraint that
        the priority field (a float) is less than or equal to maxP.
        The marshal tool must provide a get_priority method.

        Throws Queue.Empty exception if the queue is empty.

        Throws NotYet exception if the queue is not empty but the next
        record's priority is greater than maxP.
        """
        self.semaphore.acquire()
        try:
            if self._multiprocessing:
                # load caches from disk
                self._open()
            if len(self.get_cache) == 0:
                # load next cache file
                self._join()
            # in-memory cache is fresh, so if empty:
            if len(self.get_cache) == 0:
                raise Queue.Empty
            # not empty, so consider next record
            line = self.get_cache.pop(0)
            if maxP is None or \
                    self.marshal.get_priority(line) <= maxP:
                return line
            else:
                # rejecting it because of maxP priority
                self.get_cache.insert(0, line)
                raise NotYet
        finally:
            if self._multiprocessing:
                self._close()
            self.semaphore.release()

    def make_multiprocess_safe(self):
        """
        Cause future calls to get/put to _open and _close the
        in-memory information.  This allows multiple processes to
        interact with the queue.
        """
        self._multiprocessing = True

    def make_singleprocess_safe(self):
        """
        Unset the effects of make_multiprocess_safe, so that only one
        process can interact with the PersistentQueue.
        """
        self._multiprocessing = False

    def _close(self):
        """
        Sync the caches and remove any temp_path
        """
        self._sync()
        if os.path.exists(self.temp_path):
            try:
                os.remove(self.temp_path)
            except:
                pass
        self.put_cache = None
        self.get_cache = None

    def close(self):
        """
        Close the queue.  Implicitly synchronizes memory caches to disk.
        No further accesses should be made through this queue instance.
        """
        self.semaphore.acquire()
        try:
            self._close()
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
