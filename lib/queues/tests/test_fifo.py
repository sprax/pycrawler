"""
Tests for PersistentQueue.FIFO
"""
#$Id: $
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import os
import sys
import shutil
from time import time
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

def test_fifo(num, cache_size):
    test_dir = "test_dir"
    rmdir(test_dir)

    fifo = PersistentQueue.FIFO(test_dir, cache_size=cache_size)

    start = time()
    for i in xrange(num):
        fifo.put(str(i))
    fifo.close()
    elapsed_put = time() - start

    start = time()
    fifo = PersistentQueue.FIFO(test_dir, cache_size=10)
    for i in xrange(num):
        val = fifo.get()
        assert str(i) == val, "\n\nval: %s != %s" % (val, str(i))
    fifo.close()
    elapsed_get = time() - start

    assert len(os.listdir("test_dir/data/")) == 2
    rmdir(test_dir)
    print "%.1f put/sec, %.1f get/sec" % (num / elapsed_put, num / elapsed_get)
    

parser = OptionParser()
parser.add_option("-n", "--num", type=int, default=5, dest="num")
parser.add_option("-c", "--cache_size", type=int, default=5, dest="cache_size")
(options, args) = parser.parse_args()

test_fifo(options.num, options.cache_size)
