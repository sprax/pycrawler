#!/usr/bin/python2.6
"""
Provides a file-based Mutex
"""
# $Id$
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
__maintainer__ = "John R. Frank"
import os
import errno
import fcntl
import traceback

class Mutex(object):
    """
    Class wrapping around a file that is used as a mutex.
    """
    def __init__(self, lock_path=None, 
                 acquire_callback=None, release_callback=None):
        """
        If lock_path is None, then this Mutex is a FAKE, so acquire()
        returns True without doing anything, release() returns without
        doing anything, and available() always returns True.

        If lock_path is a string, then this checks if a file exists at
        that path.  If so, then this points to it without attempting
        to acquire a lock.

        If a file exists at lock_path does not exist, this creates it
        and leaves it unlocked.

        If defined, the acquire_callback is a function that take no
        arguments and gets called by acquire() after acquiring the
        file lock.  Similarly for release_callback, except it is
        called before releasing the file lock.
        """
        if lock_path is None:
            # make a FAKE mutex
            self.acquire = lambda x=None, y=None: True
            self.release = lambda x=None: None
            self.available = lambda x=None: True
            return
        self._lock_path = lock_path
        self._acquire_callback = acquire_callback
        self._release_callback = release_callback
        self._fh = None

        parent = os.path.dirname(lock_path)
        try:
            os.makedirs(parent)
        except OSError, exc:
            # okay if other process has just created it
            if exc.errno != errno.EEXIST:
                raise

        if os.path.exists(lock_path):
            assert os.path.isfile(lock_path), \
                "lock_path must be a regular file"

        # create the file, but do not block, in case another
        # process is doing this.
        self.acquire(False)
        self.release()

    def __acquire(self, block=True):
        """
        Internal function to acquire a lock, and return the
        locked file handle.
        """

        fd = os.open(self._lock_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND)
        fh = os.fdopen(fd, "a")
        lock_flags = fcntl.LOCK_EX
        if not block:
            lock_flags |= fcntl.LOCK_NB
        try:
            fcntl.flock(fd, lock_flags)
        except IOError, e:
            if e.errno in (errno.EACCES, errno.EAGAIN):
                fh.close()
                return None
            raise
        return fh

    def acquire(self, block=True):
        """
        Open the file handle, and get an exclusive lock.  Returns True
        when acquired.

        If 'block' is False, then return False if the lock was not
        acquired.  Otherwise, returns True.
        """
        fh = self.__acquire(block=block)
        if not fh:
            return False
        self._fh = fh
        if self._acquire_callback is not None:
            self._acquire_callback()
        return True

    def __release(self, fh):
        """
        Internal function to release a lock given a file handle to it.
        """
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        fh.close()

    def release(self):
        """
        Releases lock and closes file handle.  Can be called multiple
        times and will behave as though called only once.
        """
        if self._fh is not None:
            if self._release_callback is not None:
                self._release_callback()
            self.__release(self._fh)
            self._fh = None

    def available(self):
        "returns bool whether mutex is not locked"

        # We use a different file handle here so that a process
        # can call available without losing its own locks.
        acquired_fh = self.__acquire(block=False)
        if acquired_fh:
            self.__release(acquired_fh)
            return True
        else:
            return False

