"""
TriQueue provides a Queue interface to a set of three PersistentQueues:

   inQ

   readyQ

   pendingQ

Calling TriQueue.get() retrieves a record from the readyQ and stores a
copy of it in the pendingQ.  Calling TriQueue.put() puts a record in
the inQ.

At any time, one can call TriQueue.sync() to merge all the inQ and
pendingQ records into readyQ.  Generally, one does this when readyQ
has reached Empty and any process that might have removed a record
(thereby causing it to go into pending) has either put a updated
version of it back into the inQ or has crashed such that we never
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
import LineFiles
import traceback
import multiprocessing
from time import sleep
from syslog import syslog, openlog, setlogmask, LOG_UPTO, LOG_INFO, LOG_DEBUG, LOG_NOTICE, LOG_NDELAY, LOG_CONS, LOG_PID, LOG_LOCAL0
from PersistentQueue import PersistentQueue, Mutex

class TriQueue:
    debug = True

    class ReadyToSync(Exception): pass
    class Syncing(Exception): pass
    class Blocked(Exception): pass

    def __init__(self, data_path, marshal=LineFiles):
        self.data_path = data_path
        self.inQ_path = os.path.join(data_path, "inQ")
        self.readyQ_path = os.path.join(data_path, "readyQ")
        self.pendingQ_path = os.path.join(data_path, "pendingQ")
        self.marshal = marshal
        self.inQ = None
        self.readyQ = None
        self.pendingQ = None
        self._maybe_data = None
        self.open_queues()
        self.mutex_path = os.path.join(data_path, "lock_file")
        self.mutex = Mutex(self.mutex_path)
        self.sync_pending_mutex_path = os.path.join(
            data_path, "sync_lock_file")
        self.sync_pending = Mutex(
            self.sync_pending_mutex_path)

    def open_queues(self):
        """
        Open the three queues
        """
        self.inQ = PersistentQueue(self.inQ_path, marshal=self.marshal)
        self.readyQ = PersistentQueue(self.readyQ_path, marshal=self.marshal)
        self.pendingQ = PersistentQueue(self.pendingQ_path, marshal=self.marshal)
        
    def close(self):
        """
        Close all three queues that we have openned.
        """
        self.mutex.acquire()
        self.inQ.close()
        self.readyQ.close()
        self.pendingQ.close()
        self.mutex.release()

    def put(self, data, block=True):
        acquired = self.mutex.acquire(block)
        if not acquired:
            raise self.Blocked
        self.inQ.put(data)
        self.mutex.release()

    def put_nowait(self, data):
        return self.put(data, block=False)

    def get(self, block=True, maxP=None, maybe=False):
        """
        Get a data item from the readyQ and store a copy in pendingQ

        If readyQ is Empty, but we are syncing, then raise Syncing
        instead of Queue.Empty.

        If maxP is a float, then readyQ might raise NotYet
        
        If 'maybe' is True, this leaves the mutex acquired and
        does not put data into pendingQ.  The caller must call
        reject() or keep() before any other calls to this TriQueue.
        """
        acquired = self.mutex.acquire(block)
        if not acquired:
            raise self.Blocked
        maybe_got_data = False
        try:
            data = self.readyQ.get(maxP=maxP)
            if maybe:
                maybe_got_data = True
                self._maybe_data = data
            else:
                self.pendingQ.put(data)
            return data
        except NormalQueue.Empty:
            if not self.sync_pending.available():
                raise self.Syncing
            else:
                self.readyQ.make_singleprocess_safe()
            #syslog("in the mix: %d %d %d %s" % (len(self.inQ), len(self.readyQ), len(self.pendingQ), self.data_path))
            if (len(self.inQ) + len(self.pendingQ)) > 0:
                raise self.ReadyToSync
            else:
                raise NormalQueue.Empty
        finally:
            if not maybe_got_data:
                self.mutex.release()

    def get_nowait(self):
        """
        Calls get with block=False
        """
        return self.get(block=False)

    def get_maybe(self, block=True, maxP=None):
        """
        Calls get with maybe=True
        """
        return self.get(block=block, maxP=maxP, maybe=True)

    def keep(self):
        """
        Called after calling get_maybe (or get with maybe flag set).

        Puts the _maybe_data into pendingQ.

        Releases the mutex.
        """
        if self._maybe_data is None: return
        self.pendingQ.put(self._maybe_data)
        self._maybe_data = None
        self.mutex.release()

    def reject(self):
        """
        Called after calling get_maybe (or get with maybe flag set).

        Puts the _maybe_data back into the readyQ, but at the end,
        thus destroying its heap-like nature.  This allows other
        records in the readyQ to get checked.

        Releases the mutex.
        """
        if self._maybe_data is None: return
        self.readyQ.put(self._maybe_data)
        self._maybe_data = None
        self.mutex.release()

    def sync(self, block=True):
        """
        First, move the three queues out of the way and setup new
        (empty) queues to continue handling gets and puts.

        Then, merge all data from the existing three queues and put
        the result into the new readyQ.
        """
        acquired = self.mutex.acquire(block)
        if not acquired:
            raise self.Blocked
        acquired = self.sync_pending.acquire(block)
        if not acquired:
            # sync is already running
            self.mutex.release()
            raise self.Syncing
        # release sync_pending so Merger child process can acquire it.
        # This is inside the mutex.acquire, so no other process
        # can get confused about whether we're syncing.
        self.sync_pending.release()
        assert self.sync_pending.fh is None
        self.sync_pending.acquire()
        self.sync_pending.release()

        # the mutex is acquired, so put/get will block while we
        # rename those directories and setup new versions of queues
        self.inQ.close()
        self.readyQ.close()
        self.pendingQ.close()
        inQ_syncing = self.inQ_path + "_syncing"
        readyQ_syncing = self.readyQ_path + "_syncing"
        pendingQ_syncing = self.pendingQ_path + "_syncing"
        shutil.move(self.inQ_path, inQ_syncing)
        shutil.move(self.readyQ_path, readyQ_syncing)
        shutil.move(self.pendingQ_path, pendingQ_syncing)
        #map(lambda line: syslog(LOG_NOTICE, line), os.listdir(self.data_path))
        self.open_queues()
        self.readyQ.make_multiprocess_safe()

        # while still holding the mutex, we launch a process to
        # sort and merge all the files.  The child process acquires
        # the sync_pending mutex, which we checked above.
        class Merger(multiprocessing.Process):
            "manages the merge"
            name = "MergerProcess"
            marshal = self.marshal
            paths = [inQ_syncing, readyQ_syncing, pendingQ_syncing]
            sync_pending_mutex_path = self.sync_pending_mutex_path
            accumulator = self.accumulator
            readyQ = self.readyQ
            debug = self.debug
            def run(self):
                "waits for merge to complete"
                try:
                    openlog(self.name, LOG_NDELAY|LOG_CONS|LOG_PID, LOG_LOCAL0)
                    if not self.debug:
                        setlogmask(LOG_UPTO(LOG_INFO))
                    self.sync_pending = Mutex(self.sync_pending_mutex_path)
                    self.sync_pending.acquire()
                    pq = PersistentQueue(self.paths[0], marshal=self.marshal)
                    queues = [PersistentQueue(self.paths[1], marshal=self.marshal),
                              PersistentQueue(self.paths[2], marshal=self.marshal)] 
                    # try/except the sort, so if it fails we can avoid
                    # removing directories for forensic analysis
                    failure = True
                    try:
                        retval = pq.sort(merge_from=queues, merge_to=self.readyQ)
                        assert retval is True, \
                            "Should get True from sort, instead: " + str(retval)
                        pq.close()
                        failure = False
                    except Exception, exc:
                        # send full traceback to syslog in readable form
                        map(lambda line: syslog(LOG_NOTICE, line), 
                            traceback.format_exc(exc).splitlines())
                    if failure:
                        syslog(LOG_NOTICE, "Failed merge of " + str(self.paths))
                    else:
                        for path in self.paths:
                            try:
                                shutil.rmtree(path)
                            except Exception, exc:
                                # send full traceback to syslog in readable form
                                map(lambda line: syslog(LOG_NOTICE, line), 
                                    traceback.format_exc(exc).splitlines())
                    self.sync_pending.release()
                except Exception, exc:
                    # send full traceback to syslog in readable form
                    map(lambda line: syslog(LOG_NOTICE, line), 
                        traceback.format_exc(exc).splitlines())
        # end of Merger definition
        merger = Merger()
        merger.start()
        # loop until the child process acquires its mutex
        while merger.is_alive():
            acquired = self.sync_pending.acquire(block=False)
            if not acquired:
                break
            else:
                self.sync_pending.release()
                sleep(0.1)
        # now release main mutex and get back to normal operation
        self.mutex.release()
        return merger

    def sync_and_close(self):
        """
        Starts a child process that syncs this TriQueue and then
        closes it.
        """
        class SyncAndClose(multiprocessing.Process):
            debug = True
            name = "SyncAndClose: %s" % self.data_path
            TriQueue = self
            def run(self):
                "calls sync and waits for it to finish before closing"
                try:
                    openlog(self.name, LOG_NDELAY|LOG_CONS|LOG_PID, LOG_LOCAL0)
                    self.TriQueue.sync()
                    while not self.TriQueue.sync_pending.available():
                        sleep(2)
                    self.TriQueue.close()
                    syslog(LOG_DEBUG, "Done syncing and closing")
                except Exception, exc:
                    # send full traceback to syslog in readable form
                    map(lambda line: syslog(LOG_NOTICE, line), 
                        traceback.format_exc(exc).splitlines())
        sac = SyncAndClose()
        sac.start()

    def accumulator(self, line, previous):
        """
        This is an example accumulator that de-duplicates lines that
        have the same third character.
        """
        parts = line.split("")
        prev_parts = previous.split("")
        if parts[3] != prev_parts[3]:
            # we got a new one, so return the line as 'next' so we get
            # it as 'previous' in the upcoming iteration
            return line, previous
        else:
            # keep the first one that had a distinct third part
            return None, previous
