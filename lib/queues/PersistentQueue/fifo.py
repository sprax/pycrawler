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
import stat
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
    def __init__(self):
        """
        Create a FIFO of flat files in data_path/ directory.
        """
        self.reset()

    def _flush_index(self):

        """
        Ensures data stored in index file matches in-memory state
        represented by self._head and self._tail
        """
        with self._index_file(mode='w') as index_file:
            index_file.write("%d %d" % (self._head, self._tail))

    def reset(self):
        """ Point at the beginning of the queue. """
        self._reader = self._data_file(mode='r')        

    def put(self, line):
        """
        line should be a string, so it will be passed by value.  Put
        it into the tail_cache and chop off tail if beyond cache_size.
        """
        with self._data_file(mode='a') as data:
            print >>data, line

    def empty(self):
        raise Exception, "Not implemented"

    def full(self):
        """ Return False as queue is never full. """
        return False

    def get_nowait(self):
        """ get(block=False) """
        return self.get(block=False)

    def get(self, block=False, timeout=None):
        """
        Syncs the head and then returns the value that is presently at
        the head of the FIFO *and removes* it from the FIFO.

        Raises Queue.Empty if empty.
        """
        line = self._reader.readline()
        if not line:
            raise Queue.Empty
        return line.strip()

    def __iter__(self):
        "iterates over all items in FIFO, removing as it goes"
        while True:
            try:
                yield self.get()
            except Queue.Empty:
                raise StopIteration

    def sync(self):
        """
        """
        raise Exception, "Not implemented"

    def close(self):
        """
        Close the queue.  Implicitly synchronizes memory caches to disk.
        No further accesses should be made through this queue instance.
        """
        self.sync()

class FIFO(BaseFIFO):
    def __init__(self, data_path, cache_size=2**16):
        """
        Create a FIFO of flat files in data_path/ directory.

        cache_size determines maximum number of items per file.
        """

        self._data_path  = os.path.join(data_path, "data")

        try:
            os.makedirs(os.path.split(self._data_path)[0])
        except OSError, exc:
            # okay if someone else just made it:
            if exc.errno == errno.EEXIST:
                pass
            else:
                raise

        try:
            # Open just to make sure it's there.
            open(self._data_path, 'a').close()
        except IOError, exc:
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

        super(FIFO, self).__init__()

    def _data_file(self, mode='r'):
        assert mode in ('r', 'a')
        return open(self._data_path, mode + 'b')

    def sync(self):
        os.fsync(self._reader.fileno())


    def close(self):
        super(FIFO, self).close()
        self._mutex.release()

    def empty(self):
        """ Returns True if queue is empty, False otherwise. """
        return self._reader.tell() == os.fstat(self._reader.fileno())[stat.ST_SIZE]
