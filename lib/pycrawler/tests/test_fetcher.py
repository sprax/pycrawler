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

from PyCrawler import Fetcher, FetchInfoFIFO

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

    def test_fetcher(self, num=5, timeout=20):
        """ Test that fetcher can download several URLs as specified. """
        # make factories for creating surrogate HostRecord and RawFetchRecords for testing:

        raise DeprecatedTest("The URL loading is temporarily broken")

        # make temp copy of url_parts fifo and make records for all items in the FIFO
        shutil.copytree(self.url_parts_dir,
                        self.temp_dir)

        f = FetchInfoFIFO(self.temp_dir)
        hosts = {}
        count = 0
        for u in f:
            count += 1
            if count > num: break
            if u.hostname not in hosts:
                hosts[u.hostname] = host_factory.create(**{"hostname": u.hostname})
            hosts[u.hostname].data["links"].append(
                fetch_rec_factory.create(**u.__getstate__()))
            fetchrec = hosts[u.hostname].data["links"][-1]
            logging.info('Enqueued http://%s%s' % (fetchrec.hostname, fetchrec.relurl))
        f.close()
        del f

        hostQ = multiprocessing.Queue(len(hosts.keys()))
        outQ = multiprocessing.Queue()
        for hostname in hosts:
            hostQ.put(hosts[hostname])

        logging.info('%d (of %s) items enqueued.' % (len(hosts.keys()), num))

        fetcher = Fetcher(hostQ=hostQ, outQ=outQ, _debug=True)
        fetcher.start()

        t1 = time.time()

        count = 0

        failures = []

        while fetcher.is_alive() and time.time() < t1 + timeout:
            try:
                rec = outQ.get_nowait()
            except Queue.Empty:
                time.sleep(0.1)
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
                rec = outQ.get_nowait()
            except Queue.Empty:
                time.sleep(0.1)

        assert_false(failures)

def main(argv):
    parser = OptionParser()
    parser.add_option("-n", "--num", type=int, default=5, dest="num")
    (options, args) = parser.parse_args(args=argv)
    TestFetcher().test_fetcher(options.num)

if __name__ == '__main__':
    main(sys.argv)
