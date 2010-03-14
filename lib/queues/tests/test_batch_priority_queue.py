#!/usr/bin/python2.6
"""
Tests for PersistentQueue.BatchPriorityQueue
"""
#$Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import os
import sys
import copy
import Queue
import shutil
import random
import traceback
from time import sleep, time
from hashlib import md5
from optparse import OptionParser

sys.path.insert(0, os.getcwd())
import PersistentQueue

def log(msg):
    print msg
    sys.stdout.flush()

def rmdir(dir):
    if os.path.exists(dir):
        try:
            shutil.rmtree(dir)
        except Exception, exc:
            print "Did not rmtree the dir. " + str(exc)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-n", "--num", type=int, default=5, dest="num")
    parser.add_option("-c", "--cache_size", type=int, default=5, dest="cache_size")
    parser.add_option("--keep", action="store_true", default=False, dest="keep")
    (options, args) = parser.parse_args()
    num = options.num

    data_path = "test_dir"
    rmdir(data_path)

    unique_key = 4
    priority_key = 0
    HostInfo = PersistentQueue.define_record("HostInfo", "next start megabytes hits hostname")
    q = PersistentQueue.BatchPriorityQueue(
        HostInfo,
        (int, int, int, int, str),
        data_path, unique_key, priority_key,
        defaults = {"next": 0, "start": 0, "megabytes": 0, "hits": 0})

    start = time()
    for hostname in [md5(str(x)).hexdigest() for x in random.sample(xrange(num * 1000), num)]:
        next = random.choice(xrange(num * 1000))
        rec = q.put(attrs={"next": next, "hostname": hostname})

    got_exception = False
    try:
        rec = q.get()
    except q.ReadyToSync:
        got_exception = True
    assert got_exception, "failed to get ReadyToSync"

    q.sync()

    records = []
    while True:
        try:
            records.append(q.get())
        except q.Syncing:
            continue
        except Queue.Empty:
            print "Empty?"
            break
        except q.ReadyToSync:
            # expected exit point
            break
    elapsed = time() - start

    assert len(records) == num, "\nlen(records) = %d\nnum = %d" % (len(records), num)

    records_next = [rec.next for rec in records]
    records_sort = copy.copy(records_next)
    records_sort.sort()
    assert records_next == records_sort, \
        "\nanswer:   %s\n\nrecords: %s" % (records_sort, records_next)

    print "%d records: %.3f (%.1f per sec)" % \
        (len(records), elapsed, len(records) / elapsed)

    if not options.keep:
        rmdir(data_path)
