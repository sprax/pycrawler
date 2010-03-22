#!/usr/bin/python2.6
"""
CrawlStateManager provides an API to the CrawLState files
"""
# $Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
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
import cPickle as pickle
from time import time, sleep
from hashlib import md5
from PersistentQueue import define_record, FIFO, RecordFIFO, BatchPriorityQueue, b64, Static, JSON

from analyzer_chain import Analyzer, AnalyzerChain, GetLinks, SpeedDiagnostics, FetchInfo
from process import Process, multi_syslog
import url

HostRecord = define_record("HostRecord", "next start bytes hits hostname data")
HostRecord_template = (int, int, int, int, str, JSON)

class HostQueue(BatchPriorityQueue):
    """
    Maintains a persistent priority queue of HostRecord instances
    created by a RecordFactory:
    
       fields:   next, start, bytes, hits, hostname, data
       template: int,  int,   int,   int,  str,      JSON

    hostname acts as unique key.  next acts a priority.
    If next is 0, then all int fields must be 0.

    Since exists not a sufficiently memory-cheap hashing algorithm to
    detect with certainty if the system has already seen one of the
    more-than-one-billion hostnames, we only allow batch additions of
    hostnames.  Otherwise, we would have to create a host record for
    every URL the system sees, stash them on disk and periodically
    perform a massive de-duplication.  Instead, we gather hostnames
    just once -- at the same time that we import new URLs.

    As a consequence, calling report_burst() assumes that the caller
    got the hostname from this HostQueue sometime *after* the most
    recent bulk import.  Doing otherwise will corrupt the HostQueue.
    """
    def __init__(self, data_path, MAX_BYTE_RATE=2**13, MAX_HIT_RATE=1./15):
        PersistentQueue.BatchPriorityQueue.__init__(
            self, HostRecord, HostRecord_template, data_path, 
            4, # unique_key
            0, # priority_key
            defaults = {"next": 0, "start": 0, "bytes": 0, "hits": 0})
        self.MAX_BYTE_RATE = MAX_BYTE_RATE
        self.MAX_HIT_RATE  = MAX_HIT_RATE

    def report_burst(self, hostname, fetch_time, bytes, hits):
        """
        Puts a record in dumpQ with this update info for hostname
        """
        self.put(attrs={
                "hostname": hostname, "start": fetch_time, 
                "bytes": bytes, "hits": hits})

    def update_next(self, rec):
        rec.next = rec.start + \
            max(rec.bytes / self.MAX_BYTE_RATE,
                rec.hits  / self.MAX_HIT_RATE)

    def accumulate(self, records):
        """
        De-duplicates host records, keeping only one record per host
        """
        state = iter(records).next()
        for rec in records:
            if rec.hostname == state.hostname:
                state.bytes += rec.bytes
                state.hits  += rec.hits
                state.start  = min(rec.start, state.start)
            else:
                # yield accumulated value, current rec becomes state
                self.update_next(state)
                yield state
                state = rec._asdict()
        # previous state was last record, so yield it
        self.update_next(state)
        yield state

RawFetchRecord = define_record(
    "RawFetchRecord", 
    "hostid docid depth score last_modified http_response scheme hostname port relurl data")
RawFetchRecord_template = \
    (str,   str,  int,  int,  int,          int,          str,   str,     str, b64,   JSON)
FetchRecord_defaults = {"depth": 0, "score": 0, 
                        "last_modified": 0, "http_response": 0,
                        "scheme": "http", "port": "", "data": {}}

class RawFetchQueue(RecordFIFO):
    def __init__(self, base_data_path):
        data_path = os.path.join(base_data_path, "RawFetchQueue")
        RecordFIFO.__init__(
            RawFetchRecord, RawFetchRecord_template, data_path,
            defaults = FetchRecord_defaults)
    def put(self, attrs):
        attrs["hostid"], attrs["docid"] = \
            URL.get_hostid_docid(attrs["scheme"], attrs["hostname"], 
                                 attrs["port"], attrs["relurl"])
        RecordFIFO.put(**attrs)

    def put_hfr(self, hfr):
        """
        Takes a HostFetchRecord as input and stores it and all of its
        data["links"] as RawFetchRecords
        """
        if "links" in hfr.data:
            for depth, scheme, hostname, port, relurl in hfr.data["links"]:
                attrs = {"depth": depth, "scheme": scheme, 
                         "hostname": hostname, "port": port, 
                         "relurl": relurl}
                self.put(attrs)
        self.put(hfr.__getstate__())

HostFetchRecord = define_record(
    "HostFetchRecord", 
    "depth score last_modified http_response scheme hostname port relurl data")

class HostFetchQueue(RecordFIFO):
    """
    Manages the import of new URLs from the RawFetchQueue into a
    specific host's on-disk files.
    """
    def __init__(self, base_data_path, hostname):
        hostbin, hostid = URL.get_hostbin_id(hostname)
        data_path = os.path.join(base_data_path, hostbin, hostid)
        HostFetchRecord_template = \
            (int, int, int, int, str, Static(hostname), str, b64, JSON)
        RecordFIFO.__init__(
            self, HostFetchRecord, HostFetchRecord_template, data_path,
            defaults = FetchRecord_defaults)
        self.robot_path = os.path.join(data_path, "robots.txt")
        self.rp = None

        self.logger = logging.getLogger('PyCrawler.CrawlStateManager.HostFetchQueue')
        
        if os.path.exists(self.robot_path):
            self.rp = robotparser.RobotFileParser()
            try:
                self.rp.parse(
                    open(self.robot_path).read().splitlines())
            except Exception, exc:
                multi_syslog(exc, logger=self.logger.warning)
                self.rp = None

    def put_rfr(self, rfr):
        """ do something with a raw fetch record that presumably came
        from the RawFetchQueue after a merge sort, so that means this
        is an accumulator?"""
        """
        old accumulator state update:
        # same docid, so accumulate before returning
        acc_state.score += current.score
        acc_state.depth = max(
            acc_state.depth, 
            current.depth)
        if  acc_state.last_modified < current.last_modified:
            acc_state.last_modified = current.last_modified
            acc_state.http_response = current.http_response
            acc_state.content_data  = current.content_data
        """
        if self.rp is not None and not \
                self.rp.can_fetch(self.CRAWLER_NAME, URL.fullurl(rec)):
            rec.depth = ROBOTS_REJECTED
        RecordFIFO.put(self, rec)

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
    def __init__(self, go, id, inQ, packed_hostQ, config):
        """
        Setup a go Event
        """
        self.id = id
        if "debug" in config:
            self._debug = config["debug"]
        Process.__init__(self, go)
        self.inQ = inQ
        self.packed_hostQ = packed_hostQ
        self.config = config
        self.hosts_in_flight = None

        self.hostQ = None
        self.urlQ = None

        self.logger = logging.getLogger('PyCrawler.CrawlStateManager.CrawlStateManager')

    def run(self):
        """
        Moves records out of inQ and into ...
        """
        try:
            self.prepare_process()
            self.hosts_in_flight = 0
            self.hostQ = HostQueue(
                os.path.join(self.config["data_path"], "hostQ"))
            self.urlQ = PersistentQueue.FIFO(
                os.path.join(self.config["data_path"], "urlQ"))
            # main loop updates hostQ and urlQ
            while not self._stop.is_set():
                # do something to put hosts into packed_hostQ for the fetchers
                try:
                    info = self.inQ.get_nowait()
                except Queue.Empty:
                    # do something to sleep?
                    pass
                # is it a host returning from service?
                if isinstance(info, HostInfo):
                    self.hostQ.put(info)
                    self.hosts_in_flight -= 1
                    return
                # must be a FetchInfo
                assert isinstance(info, FetchInfo)
                urlQ.put(info)
                sleep(1) # slow things down
        except Exception, exc:
            multi_syslog(exc, logger=self.logger.warning)
        finally:
            self.packed_hostQ.close()
            if self.hostQ:
                self.hostQ.close()
            self.logger.debug("Exiting.")
        try:
            self.cleanup_process()
        except Exception, exc:
            multi_syslog(exc, logger=self.logger.warning)

    def make_next_packed_host(self, max_urls=10000, 
                         max_hosts=100, max_per_host=100):
        """
        Gets hosts from the hostQ, and packs it with upto max_urls
        URLs.  The number of hosts varies up to max_hosts, and the
        number of URLs per host varies up to max_per_host
        """
        num_urls = 0
        num_hosts = 0
        ready_to_sync_hosts = False
        while num_urls <= max_urls and num_hosts <= max_hosts:
            try:
                host = self.hostQ.get(max_priority=time())
                num_hosts += 1
            except BatchPriorityQueue.ReadyToSync:
                self.logger.info("ready to sync hosts")
                ready_to_sync_hosts = True
                break
            except (Queue.Empty, BatchPriorityQueue.Syncing,
                    BatchPriorityQueue.NotYet), exc:
                self.logger.info("not ready to sync hosts, because: %s" % exc)
                break
            urlQ = self.get_urlQ(host)
            try:
                num_per_host = 0
                while num_urls <= max_urls and \
                        num_per_host <= max_per_host:
                    try:
                        fetch_rec = urlQ.get()
                        num_urls += 1
                        num_per_host += 1
                        host.data["links"].append(fetch_rec)
                    except Queue.Empty:
                        break
            finally:
                urlQ.close()
            if num_per_host > 0:
                self.hosts_in_flight += 1
        if num_urls > 0:
            self.logger.info("URL count: %d" % len(host.data["links"]))
            self.packed_hostQ.put(host)
        else:
            self.logger.debug("URL count: 0")
        # outside of while loop, if hostQ is ready, and not waiting
        # for more pendings to return, then launch host sync:
        self.logger.info("ready_to_sync_hosts: %s, hosts_in_flight: %d" % (ready_to_sync_hosts, self.hosts_in_flight))
        if ready_to_sync_hosts and self.hosts_in_flight == 0:
            self.hostQ.sync()

    def get_urlQ(self, host):
        return PersistentQueue.RecordFIFO(
            "HostUrlQ", 
            "score state last_modified http_response scheme hostname port relurl data",
            (int, int, int, int, str, Static(host.hostname), str, b64, JSON),
            os.path.join(self.config["data_path"], host.hostbin, host.hostid))

    def process_urlQ(self):
        """ FIXME: info, fetch_info undefined? """

        fmc = FetchServerClient(
            self.hostbins.get_fetch_server(fetch_info.hostbin), 
            self.authkey)
        fmc.put(fetch_info)
        fmc.close()
        if self.config["hostbins"].get_fetch_server(info.hostname) != self.id:
            # pass it to FetchServer's Sender
            self.outQ.put(info)

    def get_analyzerchain(self):
        """
        Creates an AnalyzerChain with these analyzers:

            GetLinks (10x)
            CrawlStateAnalyzer (with a handle to self.inQ)
            SpeedDiagnostics

        This starts the AnalyzerChain and returns it.
        """
        class CrawlStateAnalyzer(Analyzer):
            name = "CrawlStateAnalyzer"
            csm_inQ = self.inQ
            def analyze(self, yzable):
                """
                Puts FetchInfo & HostInfo objects into csm_inQ
                """
                self.csm_inQ.put(yzable)
                return yzable
        try:
            ac = AnalyzerChain(self._go, self._debug)
            ac.append(GetLinks, 10)
            ac.append(CrawlStateAnalyzer, 1)
            ac.append(SpeedDiagnostics, 1)
            ac.start()
            return ac
        except Exception, exc:
            multi_syslog(exc, logger=self.logger.warning)
