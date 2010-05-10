#!/usr/bin/python2.6
"""
CrawlStateManager provides an API to the CrawLState files
"""

__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank.  Copyright 2010, Nokia Corporation."
__license__ = "MIT License"
__version__ = "0.1"
__revision__ = "$Id$"

import os
import sys
import copy
import gzip
import Queue
import logging
import operator
import traceback
import robotparser
import multiprocessing
from time import time, sleep
from PersistentQueue import define_record, JSON

from analyzer_chain import Analyzer, AnalyzerChain, GetLinks, LogInfo, SpeedDiagnostics, FetchInfo
from process import Process, multi_syslog
import url

HostRecord = define_record("HostRecord", ["next", "start", "bytes", "hits",
                                          "hostname", "data"])
HostRecord_template = (int, int, int, int, str, JSON)

class CrawlStateAnalyzer(Analyzer):
    name = "CrawlStateAnalyzer"

    def __init__(self, inQ, outQ, linkQ=None, **kwargs):
        super(CrawlStateAnalyzer, self).__init__(inQ, outQ, **kwargs)
        self.linkQ = linkQ

    def analyze(self, yzable):
        """
        Puts FetchInfo & HostRecord objects into csm_inQ.
        
        Get rid of data here.
        """
        if isinstance(yzable, FetchInfo):
            new_yzable = copy.copy(yzable)
            new_yzable.data = dict((k, v) for k, v in yzable.data.iteritems() \
                                               if k != 'raw_data')
            # We expect the URLChecker analyzer to drop this to prevent
            # recrawl.
            self.linkQ.put(new_yzable)

            if isinstance(yzable.links, []):
                for scheme, host, port, relurl in yzable.links:
                    link_yzable = FetchInfo.create(url='%s://%s%s%s' % (scheme, host, port and ':%s' % port or '',
                                                                   relurl or ''))
                    self.linkQ.put(link_yzable)
        return yzable

class CrawlStateManager(Process):
    """Organizes crawler's state into periodically sorted flat files

There are two time scales for rate limits with different structure:

        1) burst rates: limited in duration and total number of
        fetches, whichever comes first.  bytes-per-second and
        hits-per-second limited only by the network and remote server.

        2) long-term rates: limited long-term average MAX_BYTE_RATE
        and MAX_HIT_RATE, whichever is more restrictive.

The PyCrawler.Fetcher gets robots.txt during each burst, and holds it
only in memory.

The Analyzer created by CrawlStateManager.get_analyzerchain() passes
all records into self.inQ.  The CrawlStateManager puts HostRecords
into the HostQueue for batch sorting later.  FetchRecords are put into
the urlQ, which is periodically grouped, accumulated, and split.

urlQ is grouped first on hostid and second on docid, so we can merge
with each host's FIFOs, which includes applying RobotFileParser.

  hostid, docid, state, depth, scheme, hostname, port, relurl, inlinking docid, last_modified, http_response, content_data

    """
    MAX_HITS_RATE = 4 / 60.            # four per minute
    MAX_BYTE_RATE = 8 * 2**10          # 8 KB/sec
    MAX_STREAMED_REQUESTS = 100        # max number of streamed requests to allow before re-evaluating politeness
    #RECHECK_ROBOTS_INTERVAL = 2 * DAYS

    name = "CrawlStateManager"
    def __init__(self, go, id, fetchInfoQ, outQ, config):
        """
        Setup a go Event
        """
        self.id = id

        debug = config.get("debug", None)
        Process.__init__(self, go, debug=debug)
        self.fetchInfoQ = fetchInfoQ
        self.outQ = outQ
        self.config = config
        self.hosts_in_flight = None

    def prepare_process(self):
        super(CrawlStateManager, self).prepare_process()
        self.logger = logging.getLogger('PyCrawler.CrawlStateManager.CrawlStateManager')
        self.logger.debug('Creating CrawlStateManager')

    def run(self):
        """
        Moves records out of fetchInfoQ and into ...
        """
        self.prepare_process()

        try:
            while not self._stop.is_set():
                self.logger.debug('CrawlStateManager loop.')
                # do something to put hosts into packed_hostQ for the fetchers
                try:
                    info = self.fetchInfoQ.get_nowait()
                    self.logger.debug('Got %r' % info)
                except Queue.Empty:
                    sleep(1)
                    continue
                # must be a FetchInfo
                assert isinstance(info, FetchInfo)
                self.outQ.put(info)
        except Exception, exc:
            multi_syslog(exc, logger=self.logger.warning)
        finally:
            self.fetchInfoQ.close()
            self.logger.debug("Exiting.")
        try:
            self.cleanup_process()
        except Exception, exc:
            multi_syslog(exc, logger=self.logger.warning)


    def get_analyzerchain(self, linkQ):
        """
        Creates an AnalyzerChain with these analyzers:

            GetLinks (10x)
            CrawlStateAnalyzer (with a handle to self.fetchInfoQ)
            SpeedDiagnostics

        This starts the AnalyzerChain and returns it.
        """
        try:
            ac = AnalyzerChain(self._go, self._debug)
            ac.append(LogInfo, 1)
            ac.append(GetLinks, 10)
            ac.append(SpeedDiagnostics, 1)
            ac.append(CrawlStateAnalyzer, 1, kwargs={'linkQ': linkQ})
            ac.start()
            return ac
        except Exception, exc:
            multi_syslog(exc, logger=self.logger.warning)
