"""
CrawlState provides an API to the CrawLState files

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
import multiprocessing
import cPickle as pickle
from time import time, sleep
from hashlib import md5
from AnalyzerChain import AnalyzerChain, GetLinks, SpeedDiagnostics

def make_hostid_bin(hostkey):
    hostid = md5(hostkey).hexdigest()
    hostbin = "/".join([hostid[:2], hostid[2:4], hostid[4:6]])
    return hostid, hostbin

def make_docid(hostkey, relurl):
    docid = md5(hostkey + relurl).hexdigest()
    return docid

class CrawlStateManager(multiprocessing.Process):
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

        0) hostbin, docid, type=LINK, hostid, urlsafe_b64encode(relurl)

        1) hostbin, docid, type=FETCHED, score, depth, last_modified, http_response, link_ids, content_data

        2) hostbin, docid, type=INLINK,  score, depth, inlinking docid

Between crawls, the CrawlState.update_files() method re-organizes the
state using these steps:

        1) 'sort -u' all CrawlState.PID.urls files
        2) send to appropriate bins
        3) 'sort -u' again

        4) process each file to make a new file for each hostid that
           is stored in a directory called hostbin/hostid/urls.txt.gz

             score, depth, urlsafe_b64encode(relurl)[, last_modified, http_response, link_ids, content_data]

           where the last four fields are not required.

When that method returns, the crawler can continue.    
    """
    def __init__(self, logger, config):
        """
        Setup a go Event
        """
        multiprocessing.Process.__init__(self, name="CrawlStateManager")
        self.config = config

    def run(self):
        """
        """
        self.log(msg="in csm")
        while self.go.is_set():
            self.log(msg="looping")
            sleep(1)

    def make_analyzer(self):
        os.makedirs(self.data_path + "/CrawlState")
        output_path = self.data_path + "/CrawlState/%d.%.2f" % (self.pid, time())
        class CrawlStateAnalyzer(Analyzer):
            output_path = output_path
            name = "CrawlStateAnalyzer"
            def __init__(self, logger, output_path):
                Analyzer.__init__(self, logger)
            def prepare(self):
                self.lines = 0
                self.urls_fh = open(self.output_path + ".urls", "a")
                self.hosts_fh = open(self.output_path + ".hosts", "a")
            def analyze(self, yzable):
                if yzable.type == URLinfo_type:
                    write_URLinfo(yzable, self.urls_fh)
                    self.log(2, "wrote lines for docid: " + yzable.docid)
                elif yzable.type == HostSummary_type:
                    write_HostSummary(yzable, self.hosts_fh)
                    self.log(2, "wrote line for hostid: " + yzable.hostid)
                return yzable
            def cleanup(self):
                for fh in [self.urls_fh, self.hosts_fh]:
                    fh.flush()
                    fh.close()
        return CrawlStateAnalyzer

    def prepare(self):
        """Sort and bin all the state files
        """
        pass

    def import_received(self, files):
        """
        """
        pass

    def get_analyzerchain(self):
        
        ac = AnalyzerChain(self.logger)
        self.log(msg="adding generic Analyzers")
        ac.add_analyzer(1, GetLinks, 10)
        ac.add_analyzer(3, SpeedDiagnostics, 1)
        #self.log(msg="adding special Analyzers")
        #ac.add_analyzer(2, self.make_analyzerclass(), 1)
        self.log(msg="calling start")
        ac.start()
        return ac

    def get_packer_dump(self):
        return URL.packer().dump()

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

def write_URLinfo(yzable, url_info_filehandle):
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
    for hostkey, recs in yzable.links:
        # hostbin of the link's target host
        out_hostid, out_hostbin = make_hostid_bin(hostkey)
        for relurl, depth, last_modified, http_response, content_data in recs:
            out_docid = make_docid(hostkey, relurl)
            out_docids.append(out_docid)
            # create a row that stores just minimal info, so when we
            # sort -u, we only get one such line afterwards
            row = "|".join([out_hostbin, out_docid, LINK, hostkey + relurl]) + "\n"
            url_info_filehandle.write(row)
            # create a row that will get sorted first by hostbin,
            # and then by docid.  page_docid is here so we can
            # count each inlink just once.  The score comes from
            # the page that links out to out_docid
            row = "|".join([out_hostbin, out_docid, INLINK, str(depth),
                            yzable.score, yzable.docid]) + "\n"
            url_info_filehandle.write(row)
    # now write content_data for this fetched doc
    yzable.link_ids = ",".join(out_docids)
    yzable.hostid, yzable.hostbin = make_hostid_bin(yzable.hostkey)
    if hasattr(yzable, "content_data") and yzable.content_data:
        yzable.content_data = pickle.dumps(yzable.content_data, pickle.HIGHEST_PROTOCOL)
    else:
        yzable.content_data = ""
    yzable.rec_type = FETCHED
    # use keys ordering defined above
    row = "|".join([str(operator.attrgetter(param)(yzable))
                    for param in content_data_keys]) + "\n"
    url_info_filehandle.write(row)

hostsummary_keys = ["next_time", "hostbin", "hostkey",
                    "total_hits", "total_bytes",
                    "start_time", "end_time"]

def write_HostSummary(yzable, hostsummary_filehandle):
    yzable.hostid, yzable.hostbin = make_hostid_bin(yzable.hostkey)
    row = DELIMITER.join(
        [str(operator.attrgetter(param)(yzable))
         for param in hostsummary_keys]) + "\n"
    hostsummary_filehandle.write(row)
