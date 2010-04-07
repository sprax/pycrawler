#!/usr/bin/python2.6
"""
tests for PersistentQueue/RecordFIFO.py
"""
# $Id$
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
__maintainer__ = "John R. Frank"
import os
import sys
import errno
import shutil
from time import time

from PersistentQueue import b64, Static, JSON, RecordFIFO, define_record

def rmdir(dir):
    try:
        shutil.rmtree(dir)
    except EnvironmentError, e:
        if e.errno != errno.ENOENT:
            raise

MyRec = define_record("MyRec", ("next", "score", "depth", "data", "hostkey", "foo", "dog"))
def get_fifo(test_dir):
    "make a factory for testing"
    return RecordFIFO(
        MyRec,
        (int, float, int, b64, Static("http://www.wikipedia.com"), Static(None), JSON),
        test_dir,
        defaults = {"next": 0, "score": 0, "data": "did we survive b64ing?", "dog": {}})

class TestRecordFIFO(object):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_recordfifo(self, num=5):
        test_dir = "test_dir"
        rmdir(test_dir)

        start = time()
        fifo = get_fifo(test_dir)
        for i in xrange(num):
            fifo.put(**{"depth": i})
            fifo.put(MyRec(next=None, score=None, depth=2*i, data=None,
                           hostkey=None, foo=None, dog=None))
        fifo.close()
        elapsed_put = time() - start

        start = time()
        fifo = get_fifo(test_dir)
        for i in xrange(num):
            val = fifo.get()
            assert val.depth == i, "\n\nval: %s != %s" % (val, str(i))
            val = fifo.get()
            assert val.depth == 2*i

        fifo.close()
        elapsed_get = time() - start

        rmdir(test_dir)
        print "%.1f put/sec, %.1f get/sec" % (num / elapsed_put, num / elapsed_get)
