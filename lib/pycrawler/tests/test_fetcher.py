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
from time import time, sleep
from optparse import OptionParser
from PersistentQueue import RecordFIFO, RecordFactory, JSON, b64, define_record

sys.path.insert(0, os.getcwd())
from PyCrawler import Fetcher, CrawlStateManager


def main(argv):
    parser = OptionParser()
    parser.add_option("-n", "--num", type=int, default=5, dest="num")
    (options, args) = parser.parse_args(args=argv)
    num = options.num

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
    shutil.copytree("tests/url_parts"),
                    "url_parts_temp")
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
    shutil.rmtree("url_parts_temp")
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
            sleep(1)
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
            sleep(0.1)

    print "exiting"

if __name__ == '__main__':
    main(sys.argv)
