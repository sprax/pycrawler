#!/usr/bin/python2.6
"""
multiprocessing wrapper around pycurl
"""
#$Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__credits__ = ["The libcurl Team"]
__license__ = "MIT License"
__version__ = "0.1"

# Constants representing URL state, stored as depth values above zero
READY = 0       # 0 means a human inserted it, i.e. zero link depth
MUST_FETCH = 1  # currently only used for robots.txt
PENDING = 2
ROBOTS_REJECTED = 4
PYCURL_REJECTED = 5
REDIRECTED = 6
DEAD_LINK = 7
BAD_URL_FORMAT = 9
SCHEME_REJECTED = 10
STATE_NAMES = { 
    READY: "READY",
    MUST_FETCH: "MUST_FETCH",
    PENDING: "PENDING",
    ROBOTS_REJECTED: "ROBOTS_REJECTED",
    PYCURL_REJECTED: "PYCURL_REJECTED",
    REDIRECTED: "REDIRECTED",
    DEAD_LINK: "DEAD_LINK",
    BAD_URL_FORMAT: "BAD_URL_FORMAT",
    SCHEME_REJECTED: "SCHEME_REJECTED",
    }
import URL
import copy
import Queue
import pycurl
import logging
from time import time, sleep
from signal import signal, SIGPIPE, SIG_IGN
from Process import Process, multi_syslog
try:
    from cStringIO import StringIO
except:
    from StringIO  import StringIO

# because we use setopt(pycurl.NOSIGNAL, 1)
signal(SIGPIPE, SIG_IGN)

class Fetcher(Process):
    """
multiprocessing wrapper around pycurl (libcurl) for non-blocking,
streaming HTTP fetching.

Fetcher gets HostRecord objects through hostQ, a multiprocessing.Queue

It puts both HostRecords and FetchRecords in outQ, which is typically
the input to an AnalyzerChain.
    """
    ACCEPTED_SCHEMES = ('http', 'https', 'ftp', 'ftps', 'scp', 'sftp', 'tftp', 'telnet', 'dict', 'ldap', 'ldaps')

    FETCHER_TIMEOUT  = 600  # lifetime for all fetch operations, will force close slower operations
    CONNECT_TIMEOUT  = 20   # attempt connection timeout
    DOWNLOAD_TIMEOUT = 30   # download timeout, pycurl will end downloads that take longer
    MAX_CONNS = 128                    # num easy Curl objects to create
    MAX_CONNS_PER_HOST = 2             # num open connections allow per host
    MAX_FAILURES_PER_HOST = 5          # retires a host after this many
    FETCHES_TO_LIVE = None             # re-initializes all the libcurl objects after this many fetches

    # used in user-agent for robots.txt checking:
    CRAWLER_NAME = "PyCrawler"
    CRAWLER_HOMEPAGE = "http://www.pycrawler.org/why-is-this-thing-hitting-my-website"

    _debug = False

    def __init__(self, go=None, hostQ=None, outQ=None, params={},
                 pipelining=False, **kwargs):
        """
        If 'go' is None, then it creates and sets a go Event.

        If 'outQ' is None, then it creates one.  Usually, this is the
        inQ to an AnalyzerChain.

        Use kwargs or params to set the class parameters.  If a
        parameter appears in both kwargs and params, kwargs wins.
        """
        # allow parameters to come as a dict or as kwargs
        params.update(kwargs)
        self.__dict__.update(params)

        user_agent = "Mozilla/5.0 (%s; +%s)" % \
                     (self.CRAWLER_NAME, self.CRAWLER_HOMEPAGE)

        self.pipelining = pipelining and 1 or 0 # map to 1 or 0 int from bool
        self.pycurl_options = {
            'FOLLOWLOCATION': 1,
            'MAXREDIRS': 5,
            'CONNECTTIMEOUT': self.CONNECT_TIMEOUT,
            'TIMEOUT': self.DOWNLOAD_TIMEOUT,
            'NOSIGNAL': 1,
            'OPT_FILETIME': 1,
            'ENCODING': "",
            'DNS_CACHE_TIMEOUT': -1,
            'USERAGENT': user_agent,
            'FRESH_CONNECT': 1, # work around curl crashes from connection reuse mis-aliasing.
            }

        if not hasattr(self, "name"):
            self.name = self.CRAWLER_NAME
        Process.__init__(self, go, self._debug)

        self.logger.info("Created with useragent: %s" % user_agent)

        self.hostQ = hostQ
        self.outQ = outQ
        self.m = None             # prep for first loop
        self.fetches = 0
        #self.end_time = time() + self.FETCHER_TIMEOUT
        #self.start_time = time()
        self.idlelist = []
        self.freelist = []

    def prepare_process(self):
        Process.prepare_process(self)
        self.logger = logging.getLogger('PyCrawler.Fetcher.Fetcher')

    def init_curl(self):
        self.logger.info("pycurl.global_init...")
        # This is not threadsafe.  See note above.
        pycurl.global_init(pycurl.GLOBAL_DEFAULT)

        self.logger.info(str(pycurl.version_info()))
        self.logger.info(repr(pycurl.version))

        # This is the one (and only) CurlMulti object that this
        # Fetcher instance will use, until (unless) we pass
        # FETCHES_TO_LIVE and dereference it in cleanup()
        self.logger.debug("Creating CurlMulti object...")

        self.m = pycurl.CurlMulti()
        # Historically, there have been libcurl bugs associated with
        # pipelining.  It appears to work fine in v7.19.4.  Pipelining
        # using HTTP1.1 persistent connections with # keep-alive
        # handshaking.  
        self.m.setopt(pycurl.M_PIPELINING, self.pipelining)
        self.m.handles = []
        self.logger.debug("Allocating %d Curl objects..." % self.MAX_CONNS)
        for i in range(self.MAX_CONNS):
            try:
                c = pycurl.Curl()
            except Exception, e:
                # This has been observed to fail after exceeding
                # FETCHES_TO_LIVE and trying to dereference all the
                # libcurl objects.  Evidently that does not work.
                # Fortunately, when pycurl is running in its own
                # process without any threads, sustained fetching of
                # hundreds of thousands has been observed without
                # segfaulting or GILs,
                self.logger.error("failed pycurl.Curl(): %s" % str(e))

                # When this fails, curl is in an indeterminate state,
                # we need to stop.  This looks suspiciously like the sort
                # of things that connection re-use bugs might have caused,
                # but working around their symptoms doesn't make it not
                # corrupt the heap.
                raise

            c.fp = None
            c.host = None
            c.fetch_rec = None
            for k, v in self.pycurl_options.iteritems():
                c.setopt(getattr(pycurl, k), v)
            self.m.handles.append(c)
        self.freelist = self.m.handles[:]
        self.idlelist = []
        self.start_num_handles = 0
        self.fetches = 0

    def msg(self, step):
        msg = "%s%d/%d/%d (idle/free/total) outQ(%d) %d/%d fetches/FETCHES_TO_LIVE" % (
            step and "%s: " % step or "",
            len(self.idlelist), 
            self.m and (len(self.m.handles) - len(self.freelist)) or 0,
            self.m and len(self.m.handles) or 0,
            self.outQ and self.outQ.qsize() or 0,
            self.fetches, 
            self.FETCHES_TO_LIVE and self.FETCHES_TO_LIVE or -1)
        self.logger.debug(msg)

    def run(self):
        """
        multiprocessing.Process executes run in a separate process
        after all attributes have been pickled, sent over the wire,
        and separately instantiated here.
        """
        try:
            self.prepare_process()
            self.main()
        except Exception, exc:
            multi_syslog(exc, logger=self.logger.warning)

    def _process_errors(self, err_list, finished_list):
        """ Save error information for any error URLs,
            and mark as finished. """
        for c, errno, errmsg in err_list:
            c.fetch_rec.data["errno"]  = errno
            c.fetch_rec.data["errmsg"] = errmsg
            c.fetch_rec.data["len_fetched_data"] = 0
            c.fetch_rec.depth = DEAD_LINK
            c.host.data["failed"] += 1
            finished_list.append(c)
            self.logger.info("Failed: %s (%s) %s" % (errmsg, errno,
                                                     URL.fullurl(c.fetch_rec)))

    def _process_finished_list(self, finished_list):
        for c in finished_list:
            c.fetch_rec.data["end"]   = time()
            c.fetch_rec.http_response = c.getinfo(pycurl.RESPONSE_CODE)
            c.fetch_rec.last_modified = c.getinfo(pycurl.INFO_FILETIME)
            # maintain politeness data
            c.host.bytes += c.fetch_rec.data["len_fetched_data"]
            c.host.hits  += 1
            c.fp.close()
            c.fp = None
            c.fetch_rec = None
            self.m.remove_handle(c)  # when screwed up, this can segfault or hit a GIL
            self.start_num_handles -= 1
            self.fetches += 1  # incrementing toward periodic init_curl
            self.idlelist.append(c)  # maybe we can stream with this host

    def _process_finished_handles(self, ok_list, err_list):
        finished_list = []
        for c in ok_list:
            # effurl means redirect happened, including
            # directory redirects that simply put a '/' on
            # relurl, which is needed for
            # TextProcessing.get_links to function properly
            effurl = c.getinfo(pycurl.EFFECTIVE_URL)
            if effurl:
                # send existence of redirect to crawlstate
                redirected_fetch_rec = copy.copy(c.fetch_rec)
                redirected_fetch_rec.depth = REDIRECTED
                self.outQ.put(redirected_fetch_rec)
                try:
                    c.fetch_rec.scheme, c.fetch_rec.hostname, \
                                        c.fetch_rec.port, c.fetch_rec.relurl = \
                                        URL.get_parts(effurl)
                except Exception, exc:
                    multi_syslog(exc, logger=self.logger.warning)
                    # now what?  do something graceful...
            # store download size (maybe was compressed)
            c.fetch_rec.data["len_fetched_data"] = c.getinfo(pycurl.SIZE_DOWNLOAD)
            c.fetch_rec.data["raw_data"] = c.fp.getvalue()
            c.host.data["succeeded"] += 1
            finished_list.append(c)
            self.logger.info("%d bytes: %s" % (c.fetch_rec.data["len_fetched_data"],
                                               URL.fullurl(c.fetch_rec)))

        self._process_errors(err_list, finished_list)
        self._process_finished_list(finished_list)

    def _queue_idle_links(self):
        while self.idlelist and self._go.is_set():
            fetch_rec = None
            c = self.idlelist.pop()
            if c.host.data["failed"] >= self.MAX_FAILURES_PER_HOST \
                   and c.host.data["succeeded"] == 0:
                self.logger.info(c.host.hostkey + "too many failures, retiring.")
                self.retire(c.host)
            else:
                while fetch_rec is None and self._go.is_set():
                    self.msg("setting next URL")
                    try:
                        fetch_rec = c.host.data["links"].pop()
                    except IndexError:
                        break
                    url = URL.fullurl(fetch_rec)
                    try:
                        c.setopt(pycurl.URL, url)
                    except Exception, exc:
                        multi_syslog("URL: %s" % repr(url), exc, logger=self.logger.warning)
                        fetch_rec.depth = PYCURL_REJECTED
                        self.outQ.put(fetch_rec)
                        fetch_rec = None # loop to get another
            if fetch_rec is None:
                self.logger.debug("Disconnecting %s" % c.host.hostname)
                host = c.host
                c.host.data["conns"].remove(c)
                c.host = None
                self.freelist.append(c)
                # if was last conn for this host, then retire
                if len(host.data["conns"]) == 1:
                    self.retire(host)
                host = None
                continue  # loop again
            self.logger.debug("%s popped: %s" % (c.host.hostname, fetch_rec.relurl))
            c.fetch_rec = fetch_rec
            c.fetch_rec.last_modified = time()
            c.fp = StringIO()
            c.setopt(pycurl.WRITEFUNCTION, c.fp.write)
            self.m.add_handle(c)
            self.start_num_handles += 1

    def main(self):
        "loop until go is cleared"
        while self._go.is_set():
            self.msg("outer loop")
            if self.FETCHES_TO_LIVE is not None and \
                    self.FETCHES_TO_LIVE < self.fetches:
                self.logger.debug("past FETCHES_TO_LIVE, so purging hosts")
                self.cleanup()
            if self.m is None:
                # hosts retired, so re-initialize all curl objects:
                self.init_curl()
            assert self.start_num_handles + len(self.freelist) + \
                len(self.idlelist) == len(self.m.handles), \
                "lost track of conns: %d + %d + %d != %d" % \
                (self.start_num_handles, len(self.freelist), 
                 len(self.idlelist), len(self.m.handles))
            # try to get a host for every free curl object
            while self.freelist and self._go.is_set():
                try:
                    host = self.hostQ.get_nowait()
                except Queue.Empty:
                    break
                # use this attr of HostRecord to hold temporary data
                # using an instance of Empty class:
                host.data["conns"] = []
                host.data["failed"] = 0
                host.data["succeeded"] = 0
                # assign up to MAX_CONNS_PER_HOST
                while self.freelist and len(host.data["conns"]) < self.MAX_CONNS_PER_HOST:
                    c = self.freelist.pop()
                    c.host = host
                    host.data["conns"].append(c)
                    self.idlelist.append(c)
            self._queue_idle_links()

            # Run the internal curl state machine for the multi stack
            num_handles = self.start_num_handles
            while num_handles >= self.start_num_handles and self._go.is_set():
                self.logger.debug("perform")
                ret, num_handles = self.m.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break
            #syslog("broke out of perform loop: num_handles(%d) start_num_handles(%d) _go.is_set()->%s" %
            #       (num_handles, self.start_num_handles, self._go.is_set()))
            # Check for curl objects which have terminated, and add them to the freelist
            while self._go.is_set():
                num_q, ok_list, err_list = self.m.info_read()
                self.logger.debug("info_read: num_q(%d), ok_list(%d), err_list(%d)" % \
                                  (num_q, len(ok_list), len(err_list)))
                self._process_finished_handles(ok_list, err_list)
                if num_q == 0:
                    break
            # Currently no more I/O is pending, could do something in
            # the meantime.  We just call select() to sleep until some
            # more data is available:
            self.m.select(0.1)
        self.logger.debug("Broke out of poll loop")
        self.cleanup()
        self.logger.debug("Exiting.")

    def cleanup(self):
        """
        Removes all host and URL state from the curl objects before
        dereferencing all of them so that the python garbage collector
        can get rid of them as it sees fit.
        """
        self.logger.info("cleanup after %d fetches" % self.fetches)
        hosts = []
        if self.m is not None:
            # rescue our fetch_rec and hosts
            for c in self.m.handles:
                if c.fetch_rec is not None:
                    # happens if mid-fetch when _go clears
                    self.logger.info("cleanup: %s" % URL.fullurl(c.fetch_rec))
                    c.fetch_rec.data["raw_data"] = ""
                    c.fetch_rec.data["len_fetched_data"] = 0
                    self.outQ.put(c.fetch_rec)
                    c.fetch_rec = None
                if c.host is not None:
                    self.logger.info("cleanup: %s" % c.host.hostname)
                    hosts.append(c.host)
                    c.host.data["conns"].remove(c)
                    c.host = None
        map(self.retire, hosts)
        # simply dereference all curl objects and let gc handle it
        self.m = None
        self.idlelist = []
        self.freelist = []
        try:
            # like pycurl.global_init, this is not threadsafe
            pycurl.global_cleanup()
        except Exception, exc:  
            multi_syslog(exc, logger=self.logger.warning)

    def retire(self, host):
        # clear temp data, so host can get pickled out over outQ and
        # then garbage collected here
        host.data["conns"] = []
        self.outQ.put(host)

