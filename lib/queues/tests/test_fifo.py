"""
Tests for PersistentQueue.FIFO
"""
#$Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import os
import errno
import shutil
import tempfile
import itertools
import Queue

from nose.tools import assert_equal, assert_raises, raises

from PersistentQueue import FIFO

def rmdir(dir):
    try:
        shutil.rmtree(dir)
    except EnvironmentError, e:
        if e.errno != e.ENOENT:
            raise

class TestFIFO(object):
    def __init__(self, num=5):
        self.num = 5

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix='test_fifo.')

        try:
            fifo = FIFO(self.test_dir)

            assert fifo.full() == False

            for i in xrange(self.num):
                fifo.put(str(i))

            fifo.close()
        except:
            rmdir(self.test_dir)
            raise

    def cleanUp(self):
        rmdir(self.test_dir)

    def test_fifo_get(self):
        fifo = FIFO(self.test_dir)

        for i in xrange(self.num):
            assert not fifo.empty()
            val = fifo.get()
            assert str(i) == val, "\n\nval: %s != %s" % (val, str(i))
        assert fifo.empty()

        assert_raises(Queue.Empty, fifo.get)
        assert fifo.empty()

        fifo.close()

    def test_fifo_get_nowait(self):
        fifo = FIFO(self.test_dir)

        for i in xrange(self.num):
            assert not fifo.empty()
            val = fifo.get_nowait()
            assert str(i) == val, "\n\nval: %s != %s" % (val, str(i))
        assert fifo.empty()

        assert_raises(Queue.Empty, fifo.get_nowait)

        assert fifo.empty()

        fifo.close()

    def test_fifo_iter(self):
        fifo = FIFO(self.test_dir)

        assert not fifo.empty()
        fifo_iter = iter(fifo)
        for i, val in itertools.izip(xrange(self.num), fifo_iter):
            assert str(i) == val, "\n\nval: %s != %s" % (val, str(i))

        assert fifo.empty()

        assert_raises(StopIteration, fifo_iter.next)

        assert fifo.empty()

        fifo.close()

