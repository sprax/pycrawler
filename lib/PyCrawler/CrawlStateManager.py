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
from Fetcher import URLinfo
from Process import Process
from hashlib import md5
from AnalyzerChain import Analyzer, AnalyzerChain, GetLinks, SpeedDiagnostics, URLinfo_type, HostSummary_type
from PersistentQueue import PersistentQueue

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
for every URL in files call CrawlState.PID.urls.  Three kinds of lines
appear in CrawlState.PID.urls files:

        0) hostbin, docid, type=LINK, hostid, hostkey + relurl

        1) hostbin, docid, type=FETCHED, score, depth, last_modified, http_response, link_ids, content_data

        2) hostbin, docid, type=INLINK,  score, depth, inlinking docid

Between crawls, the CrawlState.update_files() method re-organizes the
state using these steps:

        1) 'sort -u' all CrawlState.PID.urls files
        2) send to appropriate bins
        3) 'sort -u' again

        4) process each file to make a new file for each hostid that
           is stored in a directory called hostbin/hostid/urls.txt.gz

             score, depth, hostkey + relurl [, last_modified, http_response, link_ids, content_data]

           where the last four fields are not required.

When that method returns, the crawler can continue.    
    """
    name = "CrawlStateManager"
    def __init__(self, inQ, relay, config):
        """
        Setup a go Event
        """
        Process.__init__(self)
        self.inQ = inQ
        self.relay = relay
        self.config = config
        data_path = os.path.join(config["data_path"], "urlQ")
        self.urlQ = PersistentQueue(data_path)

    def run(self):
        """
        Moves records out of inQ and into urlQ

        Keeps next_packer primed with a new packer at all times
        """
        try:
            self.prepare_process()
            while self.go.is_set():
                if self.relay.next_packer is None:
                    self.relay.next_packer = self.get_packer()
                try:
                    rec = self.inQ.get_nowait()
                    syslog(LOG_DEBUG, "got rec from inQ, putting in urlQ")
                    self.urlQ.put(rec)
                except Queue.Empty:
                    sleep(1)
            syslog(LOG_DEBUG, "Syncing inQ")
            self.urlQ.sync()
        except Exception, exc:
            syslog(LOG_NOTICE, traceback.format_exc(exc))
        syslog(LOG_DEBUG, "Exiting")

    def make_analyzer(self):
        class CrawlStateAnalyzer(Analyzer):
            name = "CrawlStateAnalyzer"
            urlQ = self.urlQ
            def analyze(self, yzable):
                if yzable.type == URLinfo_type:
                    recs = make_recs_from_URLinfo(yzable)
                    for rec in recs:
                        self.urlQ.put(rec)
                    syslog(LOG_DEBUG, "Created records for docid: " + yzable.docid)
                elif yzable.type == HostSummary_type:
                    #write_HostSummary(yzable, self.hosts_fh)
                    syslog(LOG_DEBUG, "Wrote line for hostid: " + yzable.hostkey)
                return yzable
            def cleanup(self):
                self.urlQ.sync()
        return CrawlStateAnalyzer

    def prepare_state(self):
        """Sort and bin all the state files
        """
        syslog(LOG_DEBUG, "Preparing state.")

    def get_analyzerchain(self):
        """
        Creates an AnalyzerChain with these analyzers:

            GetLinks
            SpeedDiagnostics
            CrawlStateAnalyzer (with a handle to this.urlQ)

        This starts the AnalyzerChain and returns it.
        """
        try:
            ac = AnalyzerChain()
            ac.add_analyzer(1, GetLinks, 10)
            ac.add_analyzer(2, self.make_analyzer(), 1)
            ac.add_analyzer(3, SpeedDiagnostics, 1)
            ac.start()
            return ac
        except Exception, exc:
            syslog(LOG_NOTICE, traceback.format_exc(exc))

    def get_packer(self, max=100):
        try:
            packer = URL.packer()
            count = 0
            while count < max:
                try:
                    rec = self.urlQ.get_nowait()
                except Queue.Empty:
                    break
                except Exception, exc:
                    syslog(LOG_NOTICE, traceback.format_exc(exc))
                # keep count of how many are in packer:
                count += 1
                parts = rec.split(DELIMITER)
                if parts[2] == LINK:
                    packer.add_url(parts[3])
            if count == 0:
                syslog(LOG_DEBUG, "No URLs from urlQ.")
                return None
            else:
                syslog(LOG_DEBUG, "Created a packer of length %d" % count)
                return packer
        except Exception, exc:
            syslog(LOG_NOTICE, traceback.format_exc(exc))

    def send_to_other_hostbins(self):
        # launch self.Sender to send hostbins assigned to other
        # FetchServers
        self.sender = self.Sender(
            self.id,
            self.authkey,
            self.config["hostbins"])
        self.sender.start()


# global delimiter used in all state files
DELIMITER = "|"

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

def make_hostid_bin(hostkey):
    hostid = md5(hostkey).hexdigest()
    hostbin = "/".join([hostid[:2], hostid[2:4], hostid[4:6]])
    return hostid, hostbin

def make_docid(hostkey, relurl):
    docid = md5(hostkey + relurl).hexdigest()
    return docid

def make_recs_from_url(url):
    """
    Creates an Analyzable instance for the URL and passes it to
    make_recs_from_URLinfo
    """
    hostkey, relurl = URL.get_hostkey_relurl(url)
    hostid, out_hostbin = make_hostid_bin(hostkey)
    out_docid = make_docid(hostkey, relurl)
    row = DELIMITER.join([out_hostbin, out_docid, LINK, hostkey + relurl])
    return [row]

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
    yzable.docid = make_docid(yzable.hostkey, yzable.relurl)
    # accumulate the outlinks
    out_docids = []
    # accumulate the list of records to output
    lines = []
    for hostkey, recs in yzable.links:
        # hostbin of the link's target host
        out_hostid, out_hostbin = make_hostid_bin(hostkey)
        for relurl, depth, last_modified, http_response, content_data in recs:
            out_docid = make_docid(hostkey, relurl)
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
    yzable.hostid, yzable.hostbin = make_hostid_bin(yzable.hostkey)
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

hostsummary_keys = ["next_time", "hostbin", "hostkey",
                    "total_hits", "total_bytes",
                    "start_time", "end_time"]

def write_HostSummary(yzable, hostsummary_filehandle):
    yzable.hostid, yzable.hostbin = make_hostid_bin(yzable.hostkey)
    row = DELIMITER.join(
        [str(operator.attrgetter(param)(yzable))
         for param in hostsummary_keys]) + "\n"
    hostsummary_filehandle.write(row)

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
