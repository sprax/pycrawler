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
import shutil
import multiprocessing
import time
from optparse import OptionParser
from PersistentQueue import RecordFIFO, RecordFactory, JSON, b64, define_record

from PyCrawler import Fetcher, CrawlStateManager

class TestFetcher:

    def __init__(self):
        self.temp_dir = None
        self.url_parts_dir = None

    def setUp(self):
        test_dir = os.path.dirname(os.path.abspath(__file__))
        pycrawler_dir = os.path.join(os.path.split(test_dir)[0],
                                     'url_parts_temp')
        self.temp_dir = os.path.join(pycrawler_dir, 'url_parts_temp')
        self.url_parts_dir = os.path.join(test_dir, 'url_parts')

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_fetcher(self, num=5):
        # make factories for creating surrogate HostRecord and RawFetchRecords for testing:
        host_factory = RecordFactory(
            CrawlStateManager.HostRecord, 
            CrawlStateManager.HostRecord_template,
            defaults = {"next": 0, "start": 0, "bytes": 0, "hits": 0, "data": {"succeeded": 0, "failed": 0, "links": []}})

        HostFetchRecord_template = (int, int, int, int, str, str, str, b64, JSON)
        fetch_rec_factory = RecordFactory(
            CrawlStateManager.HostFetchRecord, 
            HostFetchRecord_template,
            defaults = CrawlStateManager.FetchRecord_defaults)

        # make temp copy of url_parts fifo and make records for all items in the FIFO
        shutil.copytree(self.url_parts_dir,
                        self.temp_dir)
        UrlParts = define_record("UrlParts", "scheme hostname port relurl")
        f = RecordFIFO(UrlParts, (str, str, str, b64), "url_parts_temp")
        hosts = {}
        count = 0
        for u in f:
            count += 1
            if count > num: break
            if u.hostname not in hosts:
                hosts[u.hostname] = host_factory.create(**{"hostname": u.hostname})
            hosts[u.hostname].data["links"].append(
                fetch_rec_factory.create(**u.__getstate__()))

        time.sleep(5)

        hostQ = multiprocessing.Queue()
        outQ = multiprocessing.Queue()
        for hostname in hosts:
            hostQ.put(hosts[hostname])

        fetcher = Fetcher(hostQ=hostQ, outQ=outQ, _debug=True)
        fetcher.start()

        count = 0
        while fetcher.is_alive():
            try:
                rec = outQ.get_nowait()
            except Queue.Empty:
                time.sleep(1)
                continue
            if isinstance(rec, CrawlStateManager.HostFetchRecord):
                count += 1
            print "Done with %d of %d" % (count, num)
            if count == num:
                break

        print "done"
        fetcher.stop()
        while multiprocessing.active_children():
            print multiprocessing.active_children()
            try:
                rec = outQ.get_nowait()
            except Queue.Empty:
                time.sleep(0.1)

        print "exiting"

def main(argv):
    parser = OptionParser()
    parser.add_option("-n", "--num", type=int, default=5, dest="num")
    (options, args) = parser.parse_args(args=argv)
    TestFetcher().test_fetcher(options.num)

if __name__ == '__main__':
    main(sys.argv)
