#!/usr/bin/python2.6
"""
TriQueue provides a Queue interface to a set of three PersistentQueues:

   _inQ

   _readyQ

   _pendingQ

Calling TriQueue.get() retrieves a record from the _readyQ and stores a
copy of it in the _pendingQ.  Calling TriQueue.put() puts a record in
the _inQ.

At any time, one can call TriQueue.sync() to merge all the _inQ and
_pendingQ records into _readyQ.  Generally, one does this when _readyQ
has reached Empty and any process that might have removed a record
(thereby causing it to go into pending) has either put a updated
version of it back into the _inQ or has crashed such that we never
expect to get one.

"""
# $Id$
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
__maintainer__ = "John R. Frank"

import os
import Queue as NormalQueue
import shutil
import traceback
import multiprocessing
from time import time, sleep
from syslog import syslog, openlog, setlogmask, LOG_UPTO, LOG_INFO, LOG_DEBUG, LOG_NOTICE, LOG_NDELAY, LOG_CONS, LOG_PID, LOG_LOCAL0
from nameddict import nameddict
from PersistentQueue import PersistentQueue, Mutex

class TriQueue:
    _debug = True

    class ReadyToSync(Exception): pass
    class Syncing(Exception): pass
    class Blocked(Exception): pass

    def __init__(self, data_path, marshal=nameddict, sorting_marshal=None):
        """
        Open or create a TriQueue at the file system location
        'data_path'.  The three internal PersistentQueue instances
        will be subdirectories.

        'marshal' must be a subclass of nameddict, which is used to
        stringify the data in the queues.  This is used in the
        constructor to each of the PersistentQueue instances.

        If sorting_marshal is not None, then it is used instead of
        'marshal' in _readyQ.  This also causes a *second* sorting to
        occur during the merge process initiated by a call to sync().
        After the _inQ and _pendingQ are merged into _readyQ using the
        _sort_key defined by 'marshal', the _readyQ is sorted a second
        time using its marshal.  This means that the sorting_marshal
        must differ only slight from 'marshal', typically this means
        it differs only in the value of its _sort_key property.
        """
        self._data_path = data_path
        self._inQ_path = os.path.join(data_path, "_inQ")
        self._readyQ_path = os.path.join(data_path, "_readyQ")
        self._pendingQ_path = os.path.join(data_path, "_pendingQ")
        self._marshal = marshal
        if sorting_marshal is not None:
            self._sorting_marshal = sorting_marshal
            self._second_sort = True
        else:
            self._sorting_marshal = marshal
            self._second_sort = False
        self._inQ = None
        self._readyQ = None
        self._pendingQ = None
        self._open_queues()
        self._acquired = False
        self._mutex_path = os.path.join(data_path, "lock_file")
        self._mutex = Mutex(self._mutex_path)
        self._sync_pending_mutex_path = os.path.join(
            data_path, "sync_lock_file")
        self._sync_pending = Mutex(
            self._sync_pending_mutex_path)

    def __str__(self):
        "provide lengths of each internal queue"
        return "inQ(%d) readyQ(%d) pendingQ(%d) %s syncing" \
            % (len(self._inQ), len(self._readyQ), len(self._pendingQ),
               self._sync_pending.available() and "NOT" or "IS")

    def _open_queues(self, protect_readyQ=False):
        """
        Open the three queues
        """
        self._inQ =        PersistentQueue(self._inQ_path,      marshal=self._marshal, singleprocess=True)
        self._pendingQ =   PersistentQueue(self._pendingQ_path, marshal=self._marshal, singleprocess=True)
        self._readyQ =     PersistentQueue(self._readyQ_path,   marshal=self._sorting_marshal, singleprocess=not protect_readyQ)
        
    def close(self):
        """
        Close all three queues that we have opened.
        """
        self._mutex.acquire()
        self._inQ.close()
        self._readyQ.close()
        self._pendingQ.close()
        self._mutex.release()

    def acquire(self, block=True):
        "acquire mutex for faster get/put"
        acquired = self._mutex.acquire(block)
        if not acquired:
            raise self.Blocked
        self._acquired = True
        self._mutex.acquire()

    def release(self):
        "releases mutex"
        self._mutex.release()
        self._acquired = False

    def put(self, data, block=True):
        if not self._acquired:
            acquired = self._mutex.acquire(block)
            if not acquired:
                raise self.Blocked
        self._inQ.put(data)
        if  not self._acquired:
            self._mutex.release()

    def put_nowait(self, data):
        return self.put(data, block=False)

    def get(self, block=True, maxP=None):
        """
        Get a data item from the _readyQ and store a copy in _pendingQ

        If _readyQ is Empty, but we are syncing, then raise Syncing
        instead of Queue.Empty.

        If maxP is a float, then _readyQ might raise NotYet
        """
        if not self._acquired:
            acquired = self._mutex.acquire(block)
            if not acquired:
                raise self.Blocked
        try:
            #start = time()
            data = self._readyQ.get(maxP=maxP)
            #end = time()
            #print "%.3f seconds per _readyQ.get" % (end - start)
            self._pendingQ.put(data)
            return data
        except NormalQueue.Empty:
            if not self._sync_pending.available():
                raise self.Syncing
            elif not self._readyQ.only_singleprocess():
                # reopen readyQ with singleprocess=True
                self._readyQ.close()
                self._readyQ = PersistentQueue(
                    self._readyQ_path,   
                    marshal=self._sorting_marshal, 
                    singleprocess=True)
            if (len(self._inQ) + len(self._pendingQ)) > 0:
                # only ReadyToSync when readyQ is Empty and the other
                # two queues are not Empty
                raise self.ReadyToSync
            else:
                raise NormalQueue.Empty
        finally:
            if self._acquired:
                self._mutex.release()

    def get_nowait(self):
        """
        Calls get with block=False
        """
        return self.get(block=False)

    def sync(self, block=True):
        """
        First, move the three queues out of the way and setup new
        (empty) queues to continue handling gets and puts.

        Then, merge all data from the existing three queues and put
        the result into the new _readyQ.
        """
        acquired = self._mutex.acquire(block)
        if not acquired:
            raise self.Blocked
        acquired = self._sync_pending.acquire(block)
        if not acquired:
            # sync is already running
            self._mutex.release()
            raise self.Syncing
        # release sync_pending so Merger child process can acquire it.
        # This is inside the mutex.acquire, so no other process
        # can get confused about whether we're syncing.
        self._sync_pending.release()
        assert len(self._readyQ) == 0, "sync() requires empty _readyQ"
        # self._mutex is acquired, so put/get will block while we
        # rename directories and setup new versions of queues
        self._inQ.close()
        self._readyQ.close()  # will be reopened without change below
        self._pendingQ.close()
        # create temp paths for syncing, so can give to Merger below
        _inQ_syncing_path = self._inQ_path + "_syncing"
        _pendingQ_syncing_path = self._pendingQ_path + "_syncing"
        # move the dirs
        shutil.move(self._inQ_path, _inQ_syncing_path)
        shutil.move(self._pendingQ_path, _pendingQ_syncing_path)
        #syslog(LOG_DEBUG, "recreating three Queues as empty")
        #map(lambda line: syslog(LOG_NOTICE, line), os.listdir(self._data_path))
        self._open_queues(protect_readyQ=True)
        # while still holding the mutex, we launch a process to
        # sort and merge all the files.  The child process acquires
        # the sync_pending mutex, which we checked above.
        class Merger(multiprocessing.Process):
            "manages the merge"
            name = "MergerProcess"
            _debug = self._debug
            _TriQueue = self
            _paths = [_inQ_syncing_path, _pendingQ_syncing_path]
            def run(self):
                "waits for merge to complete"
                try:
                    openlog(self.name, LOG_NDELAY|LOG_CONS|LOG_PID, LOG_LOCAL0)
                    if not self._debug:
                        setlogmask(LOG_UPTO(LOG_INFO))
                    self._sync_pending = Mutex(self._TriQueue._sync_pending_mutex_path)
                    self._sync_pending.acquire()
                    inQ_syncing      = PersistentQueue(self._paths[0], marshal=self._TriQueue._marshal)
                    pendingQ_syncing = PersistentQueue(self._paths[1], marshal=self._TriQueue._marshal)
                    start = time()
                    if not self._TriQueue._second_sort:
                        # this uses self._marshal's accumulator
                        retval = inQ_syncing.sort(
                            merge_from=[pendingQ_syncing],
                            merge_to=self._TriQueue._readyQ)
                    else:
                        # merge into self, will resort and merge into readyQ below
                        retval = inQ_syncing.sort(merge_from=[pendingQ_syncing])
                    end = time()
                    syslog(LOG_INFO, "merge took %.1f seconds" % (end - start))
                    assert retval is True, \
                        "Should get True from sort, instead: " + str(retval)
                    if self._TriQueue._second_sort:
                        # close temp queue and reopen with new marshal
                        inQ_syncing.close()
                        start = time()
                        inQ_syncing = PersistentQueue(
                            self._paths[0], 
                            marshal=self._TriQueue._sorting_marshal,
                            singleprocess=True)
                        inQ_syncing.sort(merge_to=self._TriQueue._readyQ)
                        end = time()
                        syslog(LOG_INFO, "second sort took %.1f seconds" % (end - start))
                        assert retval is True, \
                            "Should get True from sort, instead: " + str(retval)
                    # if sort fails, the following will not happen
                    inQ_syncing.close()
                    pendingQ_syncing.close()
                    for path in self._paths:
                        shutil.rmtree(path)
                    self._sync_pending.release()
                except Exception, exc:
                    map(lambda line: syslog(LOG_NOTICE, line), 
                        traceback.format_exc(exc).splitlines())
        # end of Merger definition
        merger = Merger()
        merger.start()
        # loop until the child process acquires its mutex
        while merger.is_alive():
            acquired = self._sync_pending.acquire(block=False)
            if not acquired:
                break
            else:
                self._sync_pending.release()
                sleep(0.1)
        # now release main mutex and get back to normal operation
        self._mutex.release()
        return merger

    def close_spawn_sync_and_close(self):
        """
        Starts a child process that syncs this TriQueue and then
        closes it.
        """
        self.close()
        class SyncAndClose(multiprocessing.Process):
            name = "SyncAndClose: %s" % self._data_path
            _TriQueue = self
            def run(self):
                "calls sync and waits for it to finish before closing"
                try:
                    openlog(self.name, LOG_NDELAY|LOG_CONS|LOG_PID, LOG_LOCAL0)
                    self._TriQueue._open_queues()
                    self._TriQueue.sync()
                    while not self._TriQueue._sync_pending.available():
                        sleep(2)
                    self._TriQueue.close()
                    syslog(LOG_DEBUG, "Done syncing and closing")
                except Exception, exc:
                    # send full traceback to syslog in readable form
                    map(lambda line: syslog(LOG_NOTICE, line), 
                        traceback.format_exc(exc).splitlines())
        sac = SyncAndClose()
        sac.start()
