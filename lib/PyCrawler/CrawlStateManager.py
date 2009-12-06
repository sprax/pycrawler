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
import gzip
import Queue
import operator
import traceback
import multiprocessing
import cPickle as pickle
from time import time, sleep
from base64 import urlsafe_b64encode
from syslog import syslog, LOG_INFO, LOG_DEBUG, LOG_NOTICE
from Fetcher import HostInfo
from Process import Process
from hashlib import md5
from AnalyzerChain import Analyzer, AnalyzerChain, GetLinks, SpeedDiagnostics, FetchInfo
from PersistentQueue import PersistentQueue, LineFiles

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
    def __init__(self, id, inQ, outQ, relay, config, debug=False):
        """
        Setup a go Event
        """
        self.id = id
        if "debug" in config:
            self.debug = config["debug"]
        Process.__init__(self)
        self.inQ = inQ
        self.outQ = outQ
        self.relay = relay
        self.config = config

    def run(self):
        """
        Moves records out of inQ and into urlQ

        Keeps next_packer primed with a new packer at all times
        """
        try:
            self.prepare_process()
            self.hosts_inQ = PersistentQueue(
                os.path.join(config["data_path"], "hosts_inQ"),
                LineFiles)
            self.hosts_readyQ = PersistentQueue(
                os.path.join(config["data_path"], "hosts_readyQ"),
                LineFiles)
            self.hosts_pendingQ = PersistentQueue(
                os.path.join(config["data_path"], "hosts_pendingQ"),
                LineFiles)
            self.packerQ = PersistentQueue(
                os.path.join(config["data_path"], "packerQ"))
                # use the default, which is cPickle
            while self.go.is_set():
                if len(self.packerQ) < 5:
                    self.make_another_packer()
                if len(self.packerQ) > 0 and \
                        self.relay.next_packer is None:
                    self.relay.next_packer = self.packerQ.get()
                try:
                    url_info = self.inQ.get_nowait()
                    self.put(url_info)
                except Queue.Empty:
                    sleep(1)
            syslog(LOG_DEBUG, "Syncing inQ")
            self.hostQ.sync()
            self.packerQ.sync()
        except Exception, exc:
            syslog(LOG_NOTICE, traceback.format_exc(exc))
        syslog(LOG_DEBUG, "Exiting")

    def make_another_packer(self, max_total_urls=100, max_urls_per_host=100):
        """
        Gets hosts from the hosts_readyQ, and makes a packer with up
        to max_urls distinct URLs in it.  The number of hosts varies
        up to max_urls_per_host

        When it runs out of hosts, it waits until the fetcher stops,
        and then it merges the hosts_pendingQ and hosts_inQ together
        to make a new hosts_readyQ.

        A similar process happens for urls_inQ, urls_readyQ,
        urls_pendingQ for each host.

        """
        packer = URL.packer()
        num_urls = 0
        host_factory = HostSummary()
        while num_urls < max_total_urls:
            host = self.get_next_host()
            try:
                # get a host with next_fetch_time earlier than now
                host = self.hosts_readyQ.getif(maxP=time())
            except Queue.Empty:
                break
            host = host_factory.fromstr(host)
            host_data_path = os.path.join(
                config["data_path"], info.hostbin, info.hostid, "urls_readyQ")
            urls_readyQ = PersistentQueue(host_data_path)            
            while num_urls < max_total_urls and \
                    num_per_host < max_urls_per_host:
                try:
                    fetch_info = url_readyQ.get()
                except Queue.Empty:
                    ### do the merge of other queues to make a new
                    ### urls_readyQ for this host
                    break
                # keep count of how many are in packer:
                num_urls += 1
                packer.add_fetch_info(fetch_info)
        if count == 0:
            syslog(LOG_DEBUG, "No URLs from urlQ.")
            return None
        else:
            syslog(LOG_DEBUG, "Created a packer of length %d" % count)
            return packer

    def put(self, info):
        """
        Called on each FetchInfo object that comes through self.inQ.

        This uses the hostbin attr of info to figure it if it belongs
        to a local hostbin or a remote hostbin, and then sends it
        there.
        """
        syslog(LOG_DEBUG, "put(%s%s)" % (info.hostkey, info.relurl))
        if self.config.hostbins[info.hostbin] != self.id:
            # pass it to the Server.Sender instances that the
            # FetchServer operates:
            self.outQ.put(info)
        else:
            # belongs to this FetchServer instance
            host_data_path = os.path.join(
                config["data_path"], info.hostbin, info.hostid, "urls_inQ")
            urlQ = PersistentQueue(host_data_path)
            urlQ.put(str(info))
            urlQ.close()

    def make_analyzer(self):
        """
        Returns a subclass of Analyzer that has both self.inQ and
        self.hostQ as instance variables, so that it can stuff data
        into them as Analyzables come down the chain.

        Called by get_analyzerchain
        """
        class CrawlStateAnalyzer(Analyzer):
            name = "CrawlStateAnalyzer"
            inQ = self.inQ
            hosts_inQ = self.hosts_inQ
            def analyze(self, yzable):
                if yzable.type == URLinfo_type:
                    recs = make_recs_from_URLinfo(yzable)
                    for rec in recs:
                        self.inQ.put(rec)
                    syslog(LOG_DEBUG, "Created records for docid: " + yzable.docid)
                elif yzable.type == HostSummary_type:
                    self.hosts_inQ.put(str(yzable))
                    syslog(LOG_DEBUG, "Wrote line for hostid: " + yzable.hostkey)
                return yzable
            def cleanup(self):
                self.hostQ.sync()
        return CrawlStateAnalyzer

    def get_analyzerchain(self):
        """
        Creates an AnalyzerChain with these analyzers:

            GetLinks
            SpeedDiagnostics
            CrawlStateAnalyzer (with a handle to this.urlQ)

        This starts the AnalyzerChain and returns it.
        """
        try:
            ac = AnalyzerChain(debug=self.debug)
            ac.add_analyzer(1, GetLinks, 10)
            ac.add_analyzer(2, self.make_analyzer(), 1)
            ac.add_analyzer(3, SpeedDiagnostics, 1)
            ac.start()
            return ac
        except Exception, exc:
            syslog(LOG_NOTICE, traceback.format_exc(exc))

# end of CrawlStateManager

# This list of keys is used by both write_URLinfo and read_URLinfo so
# that the ordering of the fields is coded in just one place.  The
# purpose of rec_type is to allow the outlinks lines and content_data
# line to be written into a single file.
content_data_keys = ["hostbin", "docid", "rec_type", 
                     "score", "depth", 
                     "last_modified", "http_response", 
                     "link_ids", "content_data"]
# these are the two allowed values of rec_type
LINK    = "0"
FETCHED = "1"
INLINK  = "2"

def make_recs_from_URLinfo(yzable):
    """
    Writes three kinds of lines to url_info_filehandle.  This writes
    two lines for every link found in the page: one containing the new
    URL and another containing (yzable.docid, out_docid) and the
    score.  This also writes one line describing this page.
    """
    # ensure yzable.score is string; used in both kinds of lines.
    if hasattr(yzable, "score"):
        if isinstance(yzable.score, float):
            yzable.score = "%.3f" % yzable.score
    else:
        yzable.score = "0."
    # compute the docid for the yzable
    yzable.docid = URL.make_docid(yzable.hostkey, yzable.relurl)
    # accumulate the outlinks
    out_docids = []
    # accumulate the list of records to output
    lines = []
    for hostkey, recs in yzable.links:
        # hostbin of the link's target host
        out_hostid, out_hostbin = URL.make_hostid_bin(hostkey)
        for relurl, depth, last_modified, http_response, content_data in recs:
            out_docid = URL.make_docid(hostkey, relurl)
            out_docids.append(out_docid)
            # create a row that stores just minimal info, so when we
            # sort -u, we only get one such line afterwards
            row = DELIMITER.join([out_hostbin, out_docid, LINK, hostkey + relurl])
            lines.append(row)
            # create a row that will get sorted first by hostbin,
            # and then by docid.  page_docid is here so we can
            # count each inlink just once.  The score comes from
            # the page that links out to out_docid
            row = DELIMITER.join([out_hostbin, out_docid, INLINK, str(depth),
                            yzable.score, yzable.docid])
            lines.append(row)
    # now write content_data for this fetched doc
    yzable.link_ids = ",".join(out_docids)
    yzable.hostid, yzable.hostbin = URL.make_hostid_bin(yzable.hostkey)
    if hasattr(yzable, "content_data") and yzable.content_data:
        yzable.content_data = pickle.dumps(yzable.content_data, pickle.HIGHEST_PROTOCOL)
    else:
        yzable.content_data = ""
    yzable.rec_type = FETCHED
    # use keys ordering defined above
    row = DELIMITER.join([str(operator.attrgetter(param)(yzable))
                    for param in content_data_keys])
    lines.append(row)
    return lines

def get_all(data_path):
    queue = PersistentQueue(data_path)
    while 1:
        try:
            rec = queue.get()
        except Queue.Empty:
            break
        print rec.split(DELIMITER)
    queue.sync()

def stats(data_path):
    queue = PersistentQueue(data_path)
    print "URL queue has %d records, not de-duplicated" % len(queue)
