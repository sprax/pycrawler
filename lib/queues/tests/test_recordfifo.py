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
import shutil
from time import time
from optparse import OptionParser

sys.path.insert(0, os.getcwd())
from PersistentQueue import b64, Static, JSON, RecordFIFO, define_record

def rmdir(dir):
    if os.path.exists(dir):
        try:
            shutil.rmtree(dir)
        except Exception, exc:
            print "Did not rmtree the dir. " + str(exc)

parser = OptionParser()
parser.add_option("-n", "--num", type=int, default=5, dest="num")
parser.add_option("-c", "--cache_size", type=int, default=5, dest="cache_size")
(options, args) = parser.parse_args()
num = options.num
cache_size = options.cache_size

test_dir = "test_dir"
rmdir(test_dir)

MyRec = define_record("MyRec", ("next", "score", "depth", "data", "hostkey", "foo", "dog"))
def get_fifo():
    "make a factory for testing"
    return RecordFIFO(
        MyRec,
        (int, float, int, b64, Static("http://www.wikipedia.com"), Static(None), JSON),
        test_dir, cache_size=cache_size,
        defaults = {"next": 0, "score": 0, "data": "did we survive b64ing?", "dog": {}})

start = time()
fifo = get_fifo()
for i in xrange(num):
    fifo.put(**{"depth": i})
fifo.close()
elapsed_put = time() - start

start = time()
fifo = get_fifo()
for i in xrange(num):
    val = fifo.get()
    assert val.depth == i, "\n\nval: %s != %s" % (val, str(i))
fifo.close()
elapsed_get = time() - start

files = os.listdir("test_dir/data/")
assert len(files) == 2, str(files)
rmdir(test_dir)
print "%.1f put/sec, %.1f get/sec" % (num / elapsed_put, num / elapsed_get)
