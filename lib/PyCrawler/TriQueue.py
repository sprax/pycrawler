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
import Queue
import shutil
import traceback
import subprocess
import multiprocessing
from time import time, sleep
from syslog import syslog, LOG_INFO, LOG_DEBUG, LOG_NOTICE
from Process import Process, multi_syslog
from PersistentQueue import PersistentQueue, LineFiles

class Syncing(Exception): pass
class Blocked(Exception): pass

class TriQueue:
    debug = True

    def __init__(self, data_path):
        self.data_path = data_path
        self.inQ_path = os.path.join(data_path, "inQ")
        self.readyQ_path = os.path.join(data_path, "readyQ")
        self.pendingQ_path = os.path.join(data_path, "pendingQ")
        self.open_queues()

        self.semaphore = multiprocessing.Semaphore()
        self.sync_pending = multiprocessing.Semaphore()

    def open_queues(self):
        """
        Open the three queues
        """
        self.inQ = PersistentQueue(self.inQ_path, marshal=LineFiles())
        self.readyQ = PersistentQueue(self.readyQ_path, marshal=LineFiles())
        self.pendingQ = PersistentQueue(self.pendingQ_path, marshal=LineFiles())

    def close(self):
        """
        Close all three queues that we have openned.
        """
        self.inQ.close()
        self.readyQ.close()
        self.pendingQ.close()

    def put(self, data, block=True):
        acquired = self.semaphore.acquire(block=False)
        if not acquired:
            raise Blocked
        self.inQ.put(data)
        self.semaphore.release()

    def put_nowait(self, data):
        return self.put(data, block=False)

    def get(self, block=True):
        """
        Get a data item from the readyQ and store a copy in pendingQ

        If readyQ is Empty, but we are syncing, then raise Syncing
        instead of Queue.Empty.
        """
        acquired = self.semaphore.acquire(block=False)
        if not acquired:
            self.semaphore.release()
            raise Blocked
        try:
            data = self.readyQ.get()
            self.pendingQ.put(data)
            return data
        except Queue.Empty:
            if self.sync_pending.get_value() is 0:
                raise Syncing
            else:
                self.readyQ.make_singleprocess_safe()
                raise Queue.Empty
        finally:
            self.semaphore.release()

    def get_nowait(self):
        return self.get(block=False)

    def sync(self, block=False):
        """
        First, move the three queues out of the way and setup new
        (empty) queues to continue handling gets and puts.

        Then, merge all data from the existing three queues and put
        the result into the new readyQ.
        """
        acquired = self.semaphore.acquire(block)
        if not acquired:
            raise Blocked
        acquired = self.sync_pending.acquire(block=False)
        if not acquired:
            # sync is already running
            self.semaphore.release()
            raise Syncing
        # release sync_pending so Merger child process can acquire it.
        # This is inside the semaphore.acquire, so no other process
        # can get confused about whether we're syncing.
        self.sync_pending.release()
        # the semaphore is acquired, so put/get will block while we
        # rename those directories and setup new versions of queues
        self.inQ.close()
        self.readyQ.close()
        self.pendingQ.close()
        inQ_syncing = self.inQ_path + ".syncing"
        readyQ_syncing = self.readyQ_path + ".syncing"
        pendingQ_syncing = self.pendingQ_path + ".syncing"
        shutil.move(self.inQ_path, inQ_syncing)
        shutil.move(self.readyQ_path, readyQ_syncing)
        shutil.move(self.pendingQ_path, pendingQ_syncing)
        self.open_queues()
        self.readyQ.make_multiprocess_safe()

        # while still holding the semaphore, we launch a process to
        # sort and merge all the files.  The child process acquires
        # the sync_pending semaphore, which we checked above.
        class Merger(Process):
            paths = [inQ_syncing, readyQ_syncing, pendingQ_syncing]
            sync_pending = self.sync_pending
            accumulator = self.accumulator
            readyQ = self.readyQ
            debug = self.debug
            def run(self):
                "waits for merge to complete"
                try:
                    self.prepare_process()
                    self.sync_pending.acquire()
                    pq = PersistentQueue(self.paths[0], marshal=LineFiles())
                    queues = [PersistentQueue(self.paths[1], marshal=LineFiles()),
                              PersistentQueue(self.paths[2], marshal=LineFiles())]
                    failure = True
                    try:
                        retval = pq.sort(merge_from=queues, merge_to=self.readyQ)
                        assert retval is True, \
                            "Should get True from sort, instead: " + str(retval)
                        pq.close()
                        failure = False
                    except Exception, exc:
                        syslog(traceback.format_exc(exc))
                    if failure:
                        syslog(LOG_NOTICE, "Failed merge of " + str(self.paths))
                    else:
                        for path in self.paths:
                            try:
                                shutil.rmtree(path)
                            except Exception, exc:
                                multi_syslog(traceback.format_exc(exc))
                    self.sync_pending.release()
                except Exception, exc:
                    multi_syslog(LOG_NOTICE, traceback.format_exc(exc))
        # end of Merger definition
        merger = Merger()
        merger.start()
        # loop until the child process acquires its semaphore
        while merger.is_alive():
            acquired = self.sync_pending.acquire(block=False)
            if not acquired:
                break
            else:
                self.sync_pending.release()
                sleep(0.1)
        # now release main semaphore and get back to normal operation
        self.semaphore.release()
        return merger

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
