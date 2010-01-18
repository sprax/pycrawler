#!/usr/bin/python2.6
"""

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
import multiprocessing
from FIFO import FIFO
from time import time, sleep
from Mutex import Mutex
from syslog import syslog, openlog, LOG_INFO, LOG_NOTICE, LOG_NDELAY, LOG_CONS, LOG_PID, LOG_LOCAL0
from RecordFactory import RecordFactory

class BatchPriorityQueue(RecordFactory):
    """
    A subclass of RecordFactory that maintains two on-disk FIFOs of
    records:

      * _pQ is ordered by records' priority_field lowest-first.
        get(max_priority) retrieves next record from this queue if its
        priority is lower than max_priority.

      * _dumpQ is periodically sorted by unique_field, merged by the
        accumulate function, and sorted again by priority_field in
        order to populate the priority queue.  put() adds records to
        the dump queue.  Calling get() also puts a copy of the record
        into the dump queue, so during the merge, the accumulate
        function will see the previous record and any new records.
        For this reason, records put back into the queue after
        processing a record gotten from the queue should present *only
        changes* relative to the gotten record, so the accumulate
        function can simply add them.

    """
    def __init__(self, record_class, template,
                 data_path, unique_key, priority_key, 
                 defaults={}, delimiter="|",
                 cache_size=2**16):
        """
        See RecordFactory for record_class, 'template', 'defaults',
        and 'delimiter'.

        'data_path' and 'cache_size' are for _pQ and _dumpQ

        unique_key and priority_key are integer indexes into 'fields'
        indicating which to attributes of records to use as unique
        keys and priorities.
        """
        RecordFactory.__init__(
            self, record_class, template, defaults, delimiter)
        self._unique_key   = unique_key
        self._priority_key = priority_key
        self._cache_size = cache_size
        self._pQ_path    = os.path.join(data_path, "pQ")
        self._pQ_sync    = os.path.join(data_path, "pQ_sync")
        self._pQ = None
        self._dumpQ_path = os.path.join(data_path, "dumpQ")
        self._dumpQ_sync = os.path.join(data_path, "dumpQ_sync") 
        self._dumpQ = FIFO(self._dumpQ_path, self._cache_size)
        # the next deserialized value to return via get
        self._next = None
        self._sync_pending_path = os.path.join(data_path, "sync_pending")
        self._sync_pending = Mutex(self._sync_pending_path)
        self._lock         = Mutex(os.path.join(data_path, "lock_file"))

    def close(self):
        """
        Acquires lock, then raises self.Syncing if a sync is in
        progress, otherwise closes internal FIFOs.
        """
        self._lock.acquire()
        try:
            if not self._sync_pending.available():
                raise self.Syncing
            self._pQ.close()
            self._dumpQ.close()
        finally:
            self._lock.release()

    class NotYet(Exception): 
        "next record excluded by max_priority"
        pass
    class Blocked(Exception):
        "another process has the mutex"
        pass
    class Syncing(Exception): 
        "sync in progress"
        pass
    class ReadyToSync(Exception): 
        "pQ empty but records exist in dumpQ"
        pass

    def get(self, max_priority=None, block=True):
        """
        If block=False and cannot acquire lock, raises self.Blocked.

        If a sync is in progress, raises self.Syncing.

        If both _pQ and _dumpQ are empty, then raise Queue.Empty.  

        If empty _pQ but not empty _dumpQ, raise self.ReadyToSync.

        If next item in _pQ has a priority less than max_priority,
        then pops it from queue and returns record.
        """
        acquired = self._lock.acquire(block)
        if not acquired:
            raise self.Blocked
        try:
            if not self._sync_pending.available():
                raise self.Syncing
            if self._pQ is None:
                self._pQ = FIFO(self._pQ_path, self._cache_size)
            if self._next is None:
                # instantiate next record without removing from pQ, raises
                # Queue.Empty when no lines in FIFO
                try:
                    line = self._pQ.next()
                    self._next = self.loads(line)
                except Queue.Empty:
                    if len(self._dumpQ) == 0:
                        raise Queue.Empty
                    else:
                        raise self.ReadyToSync
            if max_priority is None or \
                    self._next[self._priority_key] < max_priority:
                # Remove this line from _pQ and put into _dumpQ. There
                # should be no risk of this raising Queue.Empty.
                self._dumpQ.put(self._pQ.get())
                ret_next = self._next
                self._next = None
                # This is only place that get() returns:
                return ret_next
            elif max_priority is not None:
                raise self.NotYet
            else:
                raise Exception("Should never get here.")
        finally:
            self._lock.release()

    def put(self, record=None, values=None, attrs=None, block=True):
        """
        If record=None, then values or attrs is passed to
        self.create() to obtain a record.

        record is put into _dumpQ.

        If block=False and cannot acquire lock, raises self.Blocked.
        """
        acquired = self._lock.acquire(block)
        if not acquired:
            raise self.Blocked
        if record is None:
            if values is not None:
                record = self.create(*values)
            elif attrs is not None:
                record = self.create(**attrs)
            else:
                raise Exception("put without record, values, or attrs")
        self._dumpQ.put(self.dumps(record))
        self._lock.release()

    def sync(self, block=True):
        """
        Removes all records from _dumpQ and _pQ and performs sort on
        unique_key, accumulate, sort on priority_key before putting
        all records into _pQ.

        If block=False and cannot acquire lock, raises self.Blocked.
        """
        acquired = self._lock.acquire(block)
        if not acquired:
            raise self.Blocked
        try:
            acquired = self._sync_pending.acquire(block=False)
            if not acquired:
                raise self.Syncing
            # move queues to the side
            self._pQ.close()
            self._dumpQ.close()
            os.rename(self._pQ_path, self._pQ_sync)
            os.rename(self._dumpQ_path, self._dumpQ_sync)
            # set pQ to None while syncing, and reopen dumpQ
            self._pQ = None
            self._dumpQ = FIFO(self._dumpQ_path, self._cache_size)
            # Release sync lock momentarily, so merger can acquire it.
            # Get is blocked by _lock, so it won't get confused.
            self._sync_pending.release()
            # launch a child to sort, accumulate, sort
            merger = self.start_merger()
            # loop until merger acquires _sync_pending
            while merger.is_alive() and self._sync_pending.available():
                sleep(0.1)
            # now get back to normal operation
            return merger
        finally:
            self._lock.release()

    def start_merger(self):
        """
        defines, instantiates, and starts a multiprocessing.Process
        for sorting, accumulating, and sorting _dumpQ into new _pQ
        """
        class Merger(multiprocessing.Process):
            "manages the sort, accumulate, sort"
            name = "SortAccumulateSort"
            _queue = self
            def run(self):
                "executes sort, accumulate, sort"
                try:
                    openlog(self.name, LOG_NDELAY|LOG_CONS|LOG_PID, LOG_LOCAL0)
                    q = self._queue
                    _sync_pending = Mutex(q._sync_pending_path)
                    _sync_pending.acquire()
                    pQ = FIFO(q._pQ_path, q._cache_size)
                    assert len(pQ) == 0
                    pQ_data = os.path.join(q._pQ_sync, "data")
                    dumpQ_data = os.path.join(q._dumpQ_sync, "data")
                    in_files  = [os.path.join(pQ_data, cache_file)
                                 for cache_file in os.listdir(pQ_data)]
                    in_files += [os.path.join(dumpQ_data, cache_file)
                                 for cache_file in os.listdir(dumpQ_data)]
                    start = time()
                    # fast version of chained generators
                    map(pQ.put, q.sort(
                            q.accumulate(
                                q.mergefiles(
                                    in_files, q._unique_key)),
                            q._priority_key))
                    # slow version of this generator chain:
                    #merged_lines = []
                    #for x in q.mergefiles(in_files, q._unique_key):
                    #    syslog("out of mergefiles: %s" % repr(x))
                    #    merged_lines.append(x)
                    #accumulated_lines = []
                    #for x in q.accumulate(merged_lines):
                    #    syslog("out of accumulator: %s" % repr(x))
                    #    accumulated_lines.append(x)
                    #for x in q.sort(accumulated_lines, q._priority_key):
                    #    syslog("out of sort: %s" % repr(x))
                    #    pQ.put(x)
                    end = time()
                    pQ.close()
                    syslog(LOG_INFO, "merge took %.1f seconds" % (end - start))
                    shutil.rmtree(q._pQ_sync)
                    shutil.rmtree(q._dumpQ_sync)
                    _sync_pending.release()
                except Exception, exc:
                    map(lambda line: syslog(LOG_NOTICE, line), 
                        traceback.format_exc(exc).splitlines())
        merger = Merger()
        merger.start()
        return merger
