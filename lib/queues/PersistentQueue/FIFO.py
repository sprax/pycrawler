"""
PersistentQueue.FIFO provides a queue-like interface to a set of flat
files stored on disk.
"""
# $Id$
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
__maintainer__ = "John R. Frank"
import os
import errno
import Queue
from blist import blist

from mutex import Mutex

class BaseFIFO(object):
    """
    Abstract base class for FIFO.

    Provides a queue-like interface to a set of flat files.  Records
    are must already be serialized into strings, which are appended to
    the flat files as new lines.
    """
    def __init__(self, cache_size=2**16):
        """
        Create a FIFO of flat files in data_path/ directory.

        cache_size determines maximum number of items per file.
        """

        # cache size must be >= 1, otherwise won't work.
        assert cache_size >= 1

        self._cache_size = cache_size

        # setup in-memory state
        self._head = 0
        self._tail = 1

        # now setup in-memory caches
        self._head_cache = None
        self._tail_cache = None
        self._load_cache("_head_cache", self._head)
        self._load_cache("_tail_cache", self._tail)

    def _flush_index(self):

        """
        Ensures data stored in index file matches in-memory state
        represented by self._head and self._tail
        """
        with self._index_file(mode='w') as index_file:
            index_file.write("%d %d" % (self._head, self._tail))

    def _flush_cache(self, cache, num):
        """
        Writes 'cache' to a file named by 'num'
        """
        with self._cache_file(num, mode='w') as fh:
            fh.write("\n".join(cache))

    def _load_cache(self, cache_name, num):
        """
        Reads lines from file named by 'num' into cache_name, which is
        either _head_cache or _tail_cache
        """
        try:
            with self._cache_file(num) as fh:
                cache = blist(fh.read().splitlines())
        except IOError, e:
            if e.errno != errno.ENOENT:
                raise
            cache = blist()
        setattr(self, cache_name, cache)

    def __len__(self):
        """
        Return number of items in queue.
        """
        return ((self._tail - self._head - 1) * self._cache_size) + \
            len(self._head_cache) + len(self._tail_cache)

    def put(self, line):
        """
        line should be a string, so it will be passed by value.  Put
        it into the tail_cache and chop off tail if beyond cache_size.
        """
        self._tail_cache.append(line)
        assert len(self._tail_cache) <= self._cache_size, \
            "tail_cache bigger than cache_size without being flushed"
        if len(self._tail_cache) == self._cache_size:
            self._flush_cache(self._tail_cache, self._tail)
            # tail_cache is now safely on disk, so in-memory is empty:
            self._tail_cache = blist()
            # wrote tail file --> update in-memory pointer
            self._tail += 1
            self._flush_index()

    def _sync_head(self):
        """
        If there are records in the FIFO, then make sure they are
        accessible in _head_cache, otherwise raise Queue.Empty
        """
        if len(self._head_cache) == 0:
            # next_head file to load is one higher than head:
            next_head = self._head + 1
            if next_head == self._tail:
                # Current position is tail, so no files on
                # disk. Replace head_cache with tail_cache.
                self._head_cache = self._tail_cache
                self._tail_cache = blist()
                # head and tail need not move
            else:
                # load next_head file from disk
                self._load_cache("_head_cache", next_head)
                # remove previous file, because its elements were
                # returned by previous calls to get(0
                self._drop_cache(self._head)
                # update head position: it moves one up (see above)
                self._head = next_head
            # make sure head is always less than tail:
            if self._head == self._tail:
                self._head = self._tail - 1
            # fix index file
            self._flush_index()
        # in-memory cache is fresh, so if empty:
        if len(self._head_cache) == 0:
            raise Queue.Empty

    def next(self):
        """
        Syncs the head and then returns the value that is presently at
        the head of the FIFO *without removing* it from the FIFO.

        Raises Queue.Empty if empty.
        """
        self._sync_head()  # raises if empty
        return self._head_cache[0]

    def get(self):
        """
        Syncs the head and then returns the value that is presently at
        the head of the FIFO *and removes* it from the FIFO.

        Raises Queue.Empty if empty.
        """
        self._sync_head()  # raises if empty
        return self._head_cache.pop(0)

    def __iter__(self):
        "iterates over all items in FIFO, removing as it goes"
        while True:
            try:
                yield self.get()
            except Queue.Empty:
                raise StopIteration

    def sync(self):
        """
        Flush both head_cache and tail_cache to disk
        """
        if self._head_cache is None: 
            # queue is closed
            return
        self._flush_index()
        self._flush_cache(self._head_cache, self._head)
        self._flush_cache(self._tail_cache, self._tail)
        # as long as we do not change head or tail values, we can
        # leave the in-memory cache unchanged.

    def close(self):
        """
        Close the queue.  Implicitly synchronizes memory caches to disk.
        No further accesses should be made through this queue instance.
        """
        self.sync()
        self._head_cache = None
        self._tail_cache = None

    def _index_file(self, mode='r'):
        raise Exception("not implemented")

    def _cache_file(self, num, mode='r'):
        raise Exception("not implemented")

    def _drop_cache(self, num):
        raise Exception("not implemented")

class FIFO(BaseFIFO):
    def __init__(self, data_path, cache_size=2**16):
        """
        Create a FIFO of flat files in data_path/ directory.

        cache_size determines maximum number of items per file.
        """

        self._index_filename = os.path.join(data_path, "index")
        self._temp_path  = os.path.join(data_path, "tempfile")
        self._data_path  = os.path.join(data_path, "data")

        if not os.path.exists(self._data_path):
            try:
                os.makedirs(self._data_path)
            except OSError, exc:
                # okay if someone else just made it:
                if exc.errno == errno.EEXIST:
                    pass
                else:
                    raise
        # acquire mutex to protect on-disk state files
        self._mutex = Mutex(os.path.join(data_path, "lock_file"))
        acquired = self._mutex.acquire(block=False)
        if not acquired:
            raise Exception("failed to acquire mutex: %s" % self._data_path)

        super(FIFO, self).__init__(cache_size=cache_size)

        # if index exists, reset them
        if os.path.exists(self._index_filename):
            index_file = open(self._index_filename)
            index_data = index_file.read()
            index_file.close()
            # last step in try/except sets the pointers
            self._head, self._tail = \
                map(int, index_data.split(" "))

    def _index_file(self, mode='r'):
        assert mode in ('r', 'w')
        return open(self._index_filename, mode)

    def _cache_file(self, num, mode='r'):
        assert mode in ('r', 'w')
        path = os.path.join(self._data_path, str(num))
        return open(path, mode + 'b')

    def _drop_cache(self, num):
        old_head = os.path.join(self._data_path, str(num))
        if os.path.exists(old_head):
            os.remove(old_head)

    def close(self):
        super(FIFO, self).close()
        self._mutex.release()
