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
import Queue
import errno
import shutil
import logging
import tempfile
import multiprocessing
import time
from optparse import OptionParser

from PyCrawler import Fetcher, FetchInfo

from nose.tools import assert_false
from nose.exc import DeprecatedTest

class TestFetcher:

    def __init__(self):
        self.temp_dir = None
        self.url_parts_dir = None

    def setUp(self):
        test_dir = os.path.dirname(os.path.abspath(__file__))
        pycrawler_dir = os.path.join(os.path.split(test_dir)[0],
                                     'url_parts_temp')
        self.temp_dir = tempfile.mkdtemp(prefix='url_parts.', suffix='.tmp')
        self.url_parts_dir = os.path.join(test_dir, 'url_parts')

        # remove url_parts_temp if it exists
        self.tearDown()

    def tearDown(self):
        try:
            shutil.rmtree(self.temp_dir)
        except EnvironmentError, e:
            # It's okay if the file doesn't exist.
            if e.errno != errno.ENOENT:
                raise

    def test_fetcher(self, timeout=5):
        """ Test that fetcher can download several URLs as specified. """
        # make factories for creating surrogate HostRecord and RawFetchRecords for testing:

        num = 1
        url = 'http://www.google.com/robots.txt'
        f = FetchInfo.create(url)

        inQ = multiprocessing.Queue(1)
        outQ = multiprocessing.Queue()

        inQ.put(f)

        fetcher = Fetcher(inQ=inQ, outQ=outQ, _debug=True)
        fetcher.start()

        t1 = time.time()

        count = 0

        failures = []

        while fetcher.is_alive() and time.time() < t1 + timeout:
            try:
                rec = outQ.get(timeout=0.1)
            except Queue.Empty:
                continue
            if isinstance(rec, FetchInfo):
                count += 1
            logging.info("Done with %d of %d" % (count, num))
            if count == num:
                break
        else:
            if fetcher.is_alive():
                failures.append('Timed out after %d seconds!' % timeout)
            failures.append('Only got %d of %d records!' % (count, num))

        logging.info("done")
        fetcher.stop()
        while multiprocessing.active_children():
            try:
                rec = outQ.get(timeout=0.1)
            except Queue.Empty:
                continue

        assert_false(failures)

def main(argv):
    parser = OptionParser()
    parser.add_option("-n", "--num", type=int, default=5, dest="num")
    (options, args) = parser.parse_args(args=argv)
    TestFetcher().test_fetcher(options.num)

if __name__ == '__main__':
    main(sys.argv)
