"""
CrawlStateManager provides an API to the CrawLState files

"""
# $Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
"""
TODO:

Finish building this first version
"""
import os
import sys
import URL
import copy
import gzip
import Queue
import operator
import traceback
import PersistentQueue
import multiprocessing
import cPickle as pickle
from time import time, sleep
from base64 import urlsafe_b64encode
from syslog import syslog, LOG_INFO, LOG_DEBUG, LOG_NOTICE
from Fetcher import HostInfo, HostInfoSorting
from Process import Process, multi_syslog
from hashlib import md5
from AnalyzerChain import Analyzer, AnalyzerChain, GetLinks, SpeedDiagnostics, FetchInfo

class CrawlStateManager(Process):
    """Organizes crawler's state into flat files on disk, which it
sorts between crawls.

=== Aspects of per-host politeness ===

There are two time scales for rate limits with different structure:

        1) burst rates: limited in duration and total number of
        fetches, whichever comes first.  bytes-per-second and
        hits-per-second limited only by the pipe and remote server.

        2) long-term rates: limited long-term average bytes-per-second
        and hits-per-second, whichever is more restrictive.

The PyCrawler.Fetcher gets robots.txt during each burst, and holds it
only in memory.

The AnalyzerChain runs an Analyzer created by
CrawlState.make_analyzer(), and this Analyzer creates
CrawlState.PID.hosts files, which has lines of this format:

    hostbin, hostid, max_score, start_time, total_bytes, total_hits

These files require de-duplication between runs.


=== Aspects of per-URL state ===

The Analyzer created by CrawlState.make_analyzer() also stores info
for every URL in instances of PersistentQueue for each hostid.  The
records in this queue are:

  docid, state, depth, hostkey, relurl, inlinking docid, last_modified, http_response, content_data

To generate packers for the internal packerQ, the CrawlStateManager
periodically sorts and merges all the records in an individual host's
queue.
    """
    name = "CrawlStateManager"
    def __init__(self, go, id, inQ, outQ, packerQ, config):
        """
        Setup a go Event
        """
        self.id = id
        if "debug" in config:
            self._debug = config["debug"]
        Process.__init__(self, go)
        self.inQ = inQ
        self.outQ = outQ
        self.packerQ = packerQ
        self.config = config
        self.hosts_in_flight = None

    def run(self):
        """
        Moves records out of inQ and into urlQ

        Keeps next_packer primed with a new packer at all times
        """
        try:
            self.prepare_process()
            self.hosts_in_flight = 0
            self.hostQ = PersistentQueue.TriQueue(
                os.path.join(self.config["data_path"], "hostQ"),
                marshal=HostInfo,
                sorting_marshal=HostInfoSorting)
            while self._go.is_set():
                syslog("hostQ: %s" % self.hostQ)
                if self.packerQ.qsize() < 5:
                    self.make_next_packer()
                try:
                    info = self.inQ.get_nowait()
                    self.put(info)
                except Queue.Empty:
                    if self.hosts_in_flight == 0:
                        sleep(2)
                sleep(1) # slow things down
        except Exception, exc:
            multi_syslog(exc)
        finally:
            self.packerQ.close()
            self.hostQ.close()
            syslog(LOG_DEBUG, "Exiting.")

    def make_next_packer(self, max_urls=10000, 
                         max_hosts=100, max_per_host=100):
        """
        Gets hosts from the hostQ, and makes a packer with up to
        max_urls URLs.  The number of hosts varies up to max_hosts,
        and the number of URLs per host varies up to max_per_host
        """
        packer = URL.packer()
        num_urls = 0
        num_hosts = 0
        ready_to_sync_hosts = False
        while num_urls <= max_urls and num_hosts <= max_hosts:
            try:
                host = self.hostQ.get(maxP=time())
            except PersistentQueue.TriQueue.ReadyToSync:
                syslog("ready to sync hosts")
                ready_to_sync_hosts = True
                break
            except (Queue.Empty, PersistentQueue.TriQueue.Syncing, 
                    PersistentQueue.PersistentQueue.NotYet), exc:
                syslog("not ready to sync hosts, but: %s" % exc)
                break
            #syslog(LOG_DEBUG, "got host: %s, available: %s" % (host, self.hostQ._mutex.available()))
            num_hosts += 1
            urlQ = self.get_urlQ(host)
            num_per_host = 0
            while num_urls <= max_urls and \
                    num_per_host < max_per_host:
                try:
                    fetch_info = urlQ.get()
                except (Queue.Empty, PersistentQueue.TriQueue.Syncing, 
                        PersistentQueue.PersistentQueue.NotYet), exc:
                    #syslog("urlQ(%s) --> %s" % (data_path, type(exc)))
                    urlQ.close()
                    break
                except PersistentQueue.TriQueue.ReadyToSync:
                    #syslog("Syncing urlQ(%s)" % data_path)
                    urlQ.close_spawn_sync_and_close()
                    break
                # keep count of how many are in packer:
                num_urls += 1
                num_per_host += 1
                #syslog(LOG_DEBUG, "adding to packer: %s" % fetch_info)
                packer.add_fetch_info(fetch_info)
            if num_per_host > 0:
                #syslog(LOG_DEBUG, "got %d URLs --> in flight: %s" % \
                #           (num_per_host, host.hostkey))
                self.hosts_in_flight += 1
            else:
                #syslog(LOG_DEBUG, "putting host back")
                self.hostQ.put(host)
        if num_urls > 0:
            syslog("URL count: %d" % len(packer))
            self.packerQ.put(packer)
        else:
            syslog(LOG_DEBUG, "URL count: 0")
        # outside of while loop, if hostQ is ready, and not waiting
        # for more pendings to return, then launch host sync:
        syslog("ready_to_sync_hosts: %s, hosts_in_flight: %d" % (ready_to_sync_hosts, self.hosts_in_flight))
        if ready_to_sync_hosts and self.hosts_in_flight == 0:
            self.hostQ.sync()

    def get_urlQ(self, host):
        """
        Constructs a TriQueue using a subclass of FetchInfo with
        _defaults modified with the hostkey for later access by
        FetchInfo.set_external_attrs()

        This is the only place that urlQ instances are created.
        """
        data_path = os.path.join(
            self.config["data_path"], host.hostbin, host.hostid)
        new_defaults = copy.copy(FetchInfo._defaults)
        new_defaults["hostkey"] = host.hostkey
        class FetchInfoForHost(FetchInfo):
            _defaults = new_defaults
        urlQ = PersistentQueue.TriQueue(
            data_path, 
            marshal=FetchInfoForHost)
        return urlQ

    def put(self, info):
        """
        Called on each HostInfo and FetchInfo object that comes
        through self.inQ.

        This uses the hostbin attr of info to figure it if it belongs
        to a local hostbin or a remote hostbin, and then sends it
        there.
        """
        if isinstance(info, HostInfo):
            self.hostQ.put(info)
            self.hosts_in_flight -= 1
            return
        # must be a FetchInfo
        assert isinstance(info, FetchInfo)
        if self.config["hostbins"].get_fetch_server(info.hostbin) != self.id:
            # pass it to the Server.Sender instances that the
            # FetchServer operates:
            syslog(LOG_DEBUG, 
                   "outQ.put(%s%s)" % (info.hostkey, info.relurl))
            self.outQ.put(info)
        else:
            syslog(LOG_DEBUG, 
                   "urlQ(%s).put(%s) --> %s" \
                       % (info.hostkey, info.relurl, info))
            # need to add a HostInfo to the hostQ
            host_info = HostInfo({"hostkey": info.hostkey})
            # These are new host_info that might duplicate existing
            # host records, and should not increment hosts_in_flight:
            self.hostQ.put(host_info)
            syslog(LOG_DEBUG, "hostQ.put(%s)" % info.hostkey)
            # use the host to open the urlQ:
            urlQ = self.get_urlQ(host_info)
            urlQ.put(info)
            urlQ.close()
            
    def get_analyzer(self):
        """
        Returns a subclass of Analyzer that has both self.inQ and
        self.hostQ as instance variables, so that it can stuff data
        into them as Analyzables come down the chain.

        Called by get_analyzerchain
        """
        class CrawlStateAnalyzer(Analyzer):
            name = "CrawlStateAnalyzer"
            csm_inQ = self.inQ
            def prepare(self):
                syslog("CrawlStateAnalyzer prepared")

            def analyze(self, yzable):
                """
                Puts all FetchInfo and HostInfo records into
                CrawlStateManager.inQ.  Also creates new FetchInfo
                instances for all the links found in a FetchInfo.
                """
                #syslog("analyze(%s) is FetchInfo(%s) or HostInfo(%s)" % (type(yzable), isinstance(yzable, FetchInfo), isinstance(yzable, HostInfo)))
                if isinstance(yzable, FetchInfo):
                    self.csm_inQ.put(yzable)
                    finfos = yzable.make_FetchInfos_for_links()
                    for fetch_info in finfos:
                        self.csm_inQ.put(fetch_info)
                    syslog(LOG_DEBUG, "Created %d records for docid: %s" % 
                           (len(finfos), yzable.docid))
                elif isinstance(yzable, HostInfo):
                    self.csm_inQ.put(yzable)
                    syslog(LOG_DEBUG, "HostInfo: %s" % yzable.hostkey)
                return yzable
        return CrawlStateAnalyzer

    def get_analyzerchain(self):
        """
        Creates an AnalyzerChain with these analyzers:

            GetLinks (10x)
            CrawlStateAnalyzer (with a handle to this.urlQ)
            SpeedDiagnostics

        This starts the AnalyzerChain and returns it.
        """
        try:
            ac = AnalyzerChain(self._go, self._debug)
            ac.append(GetLinks, 10)
            ac.append(self.get_analyzer(), 1)
            ac.append(SpeedDiagnostics, 1)
            ac.start()
            return ac
        except Exception, exc:
            multi_syslog(exc)

# end of CrawlStateManager

def get_all(data_path):
    queue = PersistentQueue.PersistentQueue(data_path)
    while 1:
        try:
            rec = queue.get()
        except Queue.Empty:
            break
        print rec.split(DELIMITER)
    queue.sync()

def stats(data_path):
    queue = PersistentQueue.PersistentQueue(data_path)
    print "URL queue has %d records, not de-duplicated" % len(queue)
