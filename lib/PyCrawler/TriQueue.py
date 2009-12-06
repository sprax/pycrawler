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
# $Id: $
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
from PersistentQueue import PersistentQueue

class Syncing(Exception): pass

class TriQueue:
    def __init__(self, data_path):
        self.data_path = data_path
        self.inQ_path = os.path.join(data_path, "inQ")
        self.readyQ_path = os.path.join(data_path, "readyQ")
        self.pendingQ_path = os.path.join(data_path, "pendingQ")
        self.open_queues()

        self.semaphore = multiprocessing.Semaphore()
        self.sync_pending = multiprocessing.Semaphore()
        self.syncing = False

    def open_queues(self):
        self.inQ = PersistentQueue(self.inQ_path)
        self.readyQ = PersistentQueue(self.readyQ_path)
        self.pendingQ = PersistentQueue(self.pendingQ_path)

    def put(self, data):
        self.semaphore.acquire()
        self.inQ.put(data)
        self.semaphore.release()

    def get(self):
        """
        Get a data item from the readyQ and store a copy in pendingQ

        If readyQ is Empty, but we are syncing, then raise Syncing
        instead of Queue.Empty.
        """
        self.semaphore.acquire()
        try:
            data = self.readyQ.get()
        except Queue.Empty:
            if self.syncing:
                raise Syncing
            else:
                raise Queue.Empty
        self.pendingQ.put(data)
        self.semaphore.release()
        return data

    def sync(self):
        """
        First, move the three queues out of the way and setup new
        (empty) queues to continue handling gets and puts.

        Then, merge all data from the existing three queues and put
        the result into the new readyQ.
        """
        self.semaphore.acquire()
        self.syncing = self.sync_pending.acquire(block=False)
        if not self.syncing:
            # there is a sync running, so return False
            self.semaphore.release()
            return False
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
        
        # while still holding the semaphore, we launch a process to
        # sort and merge all the files.  The child process acquires
        # the sync_pending semaphore, which we checked above.
        class Merger(Process):
            paths = [inQ_syncing, readyQ_syncing, pendingQ_syncing]
            sync_pending = self.sync_pending
            accumulator = self.accumulator
            readyQ = self.readyQ
            field_begin = "+1"
            field_end   = "-2"

            def run(self):
                """
                """
                try:
                    self.prepare_process()
                    self.sync_pending.acquire()
                    sort = subprocess.Popen(
                        ["sort", "-u", "-"],
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE)
                    # get list of all individual cache files:
                    files = []
                    for path in self.paths:
                        for file_path in os.listdir(os.path.join(path, "data")):
                            files.append(os.path.join(path, "data", file_path))
                    syslog("sort process says: " + str(sort.poll()))
                    for file_path in files:
                        chunk = open(file_path)
                        while True:
                            line = chunk.readline()
                            if not line: break
                            line = line.strip()
                            try:
                                sort.communicate(line)
                            except Exception, exc:
                                syslog(LOG_NOTICE, "sort broke before terminating: " + sort.stderr.read())
                    sort.stdin.close()
                    previous = None
                    while True:
                        line = sort.stdout.readline()
                        if not line: break
                        # Iterate accumulator until it finds the next
                        # row.  It modifies previous as it iterates
                        # through a group of records that need to be
                        # merged.
                        next, previous = self.accumulator(line, previous)
                        if next is not None:
                            self.readyQ.put(previous)
                            previous = next
                    if sort.returncode is not None:
                        syslog(LOG_NOTICE, "sort broke before terminating: " + sort.stderr.read())
                        sort.kill()
                    else:
                        for path in self.paths:
                            try:
                                shutil.rmtree(path)
                            except Exception, exc:
                                multi_syslog(traceback.format_exc(exc))
                    syslog(LOG_NOTICE, "Completed merge of " + str(self.paths))
                    self.sync_pending.release()
                except Exception, exc:
                    multi_syslog(LOG_NOTICE, traceback.format_exc(exc))
        # end of Merger definition
        merger = Merger()
        merger.start()
        # loop until the child process acquired its semaphore
        while True:
            got_lock = self.sync_pending.acquire(block=False)
            if got_lock:
                self.sync_pending.release()
                sleep(0.1)
            else:
                break
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
        

if __name__ == "__main__":
    import random
    test_path = "/tmp/test"
    if os.path.exists(test_path):
        shutil.rmtree(test_path)
    tq = TriQueue(test_path)
    for i in range(1000):
        v = str(random.random())
        tq.put(v)
    tq.sync()
    i = 0
    while i < 500:
        try:
            tq.get()
            i += 1
        except Queue.Empty:
            sleep(1)
    tq.sync()
