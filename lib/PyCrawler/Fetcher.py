"""
A wrapper around pycurl that implements politeness and passes
documents into an AnalyzerChain
"""
#$Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__credits__ = ["The libcurl Team"]
__license__ = "MIT License"
__version__ = "0.1"
"""
TODO

 * cleanup the messiness around the PriorityQueue by getting rid of
   abstraction violations (using _get and _put here) and look at
   switching to Python's Queue.PriorityQueue

 * pass through information about 301 redirects, so we accumulate info
   about such URLs instead of perpetually recycling them

"""
# Constants representing URL state
READY = 0       # 0 means a human inserted it, i.e. zero link depth
MUST_FETCH = 1  # currently only used for robots.txt
PENDING = 2
ROBOTS_REJECTED = 4
PYCURL_REJECTED = 5
DEAD_LINK = 7
BAD_URL_FORMAT = 9
SCHEME_REJECTED = 10
STATE_NAMES = { 
    ROBOTS_REJECTED: "ROBOTS_REJECTED",
    PYCURL_REJECTED: "PYCURL_REJECTED",
    PENDING: "PENDING",
    MUST_FETCH: "MUST_FETCH",
    READY: "READY",
    DEAD_LINK: "DEAD_LINK",
    BAD_URL_FORMAT: "BAD_URL_FORMAT",
    SCHEME_REJECTED: "SCHEME_REJECTED",
    }

SECONDS = 1
MINUTES = 60
HOURS   = 60 * MINUTES
DAYS    = 24 * HOURS

import pycurl
#    http://curl.haxx.se/libcurl/c/curl_global_init.html
# This function is not thread safe. You must not call it when any
# other thread in the program (i.e. a thread sharing the same memory)
# is running. This doesn't just mean no other thread that is using
# libcurl. Because curl_global_init() calls functions of other
# libraries that are similarly thread unsafe, it could conflict with
# any other thread that uses these other libraries.
#
# Now that we are using multiprocessing, we can call this in the
# curl_init function instead...
#pycurl.global_init(pycurl.GLOBAL_DEFAULT)

import URL
import Queue
import robotparser
import AnalyzerChain
import PriorityQueue
from time import time, sleep
from syslog import syslog, LOG_INFO, LOG_DEBUG, LOG_NOTICE
from Process import Process, multi_syslog
try:
    from cStringIO import StringIO
except:
    from StringIO  import StringIO

# because we use setopt(pycurl.NOSIGNAL, 1)
from signal import signal, SIGPIPE, SIG_IGN
signal(SIGPIPE, SIG_IGN)

class HostInfo(AnalyzerChain.Analyzable):
    _defaults = {
        "hostkey": '',
        "total_hits": 0,
        "total_bytes": 0,
        "start_time": 0.,
        "end_time": 0.,
        "next_time": 0.,
        }
    
    _key_ordering = [
        "next_time", "hostbin", "hostkey",
        "total_hits", "total_bytes",
        "start_time", "end_time"]

    _val_types = [
        int, str, str,
        int, int,
        int, int]

    # this is used for grouping
    _sort_key = 2

    def __init__(self, attrs=None):
        """
        Creates a HostInfo with hostid and hostbin
        """
        AnalyzerChain.Analyzable.__init__(self, attrs)
        self.hostid, self.hostbin = URL.make_hostid_bin(self.hostkey)

    @classmethod
    def accumulator(cls, acc_state, line):
        """
        De-duplicates host records, keeping only one record per host
        """
        if line == "":
            # previous state was last record, so cause break
            return None, cls.dumps(acc_state)
        current = cls.loads(line)[0]
        if acc_state is None:
            # first pass accumulation
            return current, None
        if current.hostkey == acc_state.hostkey:
            # same as previous, so check which has content
            if current.total_hits == 0 and current.total_bytes == 0 \
                    and current.start_time == 0:
                # cannot be new, so just ignore it
                return acc_state, None
            elif acc_state.total_hits == 0 and acc_state.total_bytes == 0 \
                    and acc_state.start_time == 0:
                # current represents a populated record for this host,
                return current, None
            else:
                # pendingQ hitting inQ replacement, aggregate data:
                current.total_bytes += acc_state.total_bytes
                current.total_hits  += acc_state.total_hits
                current.start_time = max(current.start_time, acc_state.start_time)
                # bad, very bad: abstraction barriers all messed up
                current.next_time =  current.start_time + \
                    max(current.total_bytes / Fetcher.MAX_BYTE_RATE,
                        current.total_hits  / Fetcher.MAX_HITS_RATE)
                return current, None
        else:
            # new one! give back a serialized form as second value,
            # and 'current' becomes the acc_state:
            return current, cls.dumps(acc_state)

# this is used for the second sort in the hostsQ (a TriQueue)
class HostInfoSorting(HostInfo):
    _sort_key = 0
    accumulator = None

class Host(PriorityQueue.Queue):
    """
    A simple class for holding information about an individual host.
    Always attached to a particular Fetcher instance.
    """
    def __init__(self, fetcher, hostkey, relurls):
        PriorityQueue.Queue.__init__(self)
        self.fetcher = fetcher
        self.hostkey = hostkey
        self.total_bytes = 0
        self.total_hits  = 0
        self.robots_next = 0
        self.rp = None
        self.conns = []
        self.start_time = None
        self.scheduled = False
        self.failed = 0
        self.succeeded = 0
        for (relurl, depth, last_modified, http_response, content_data) in relurls:
            # depth prioritizes relurls.  In principle, this should be
            # something more general.
            self.put((depth, 
                      AnalyzerChain.FetchInfo({
                            "hostkey": hostkey, 
                            "relurl":  relurl, 
                            "depth":   depth, 
                            "last_modified": last_modified,
                            })))
            
    def _get(self):
        """set self.start_time before calling PriorityQueue.Queue.get()"""
        if self.start_time is None: 
            self.start_time = time()
        if self.robots_next == 0:
            # have not fetched robots.txt for first time yet
            if self.top()[0] != "/robots.txt":
                return (None, None)
        return PriorityQueue.Queue._get(self)

    def _put(self, depth_and_url_info):
        """
        check if we need to get robots.txt before calling PriorityQueue.Queue.put
        """
        # If time, put robots at top of queue. NB this possibly
        # changed robots file will not be applied to URLs already in
        # the queue.
        if 0 <= self.robots_next < time():
            self.robots_next = -1  # set to pending
            syslog(LOG_DEBUG, "put(%s/robots.txt)" % self.hostkey)
            robots_info = AnalyzerChain.FetchInfo({
                    "hostkey": self.hostkey, 
                    "relurl":  "/robots.txt",
                    "depth":  1,  # meaning one above a human-created seed
                    "last_modified": 0,
                    "state": MUST_FETCH })
            PriorityQueue.Queue._put(self, (MUST_FETCH, robots_info))
        # Use robots.txt (if exists) to decide if can fetch url
        (depth, url_info) = depth_and_url_info
        if not self.robots_allows(url_info.relurl):
            url_info.state = ROBOTS_REJECTED
            self.fetcher.out_proc(url_info)
        else:
            syslog(LOG_DEBUG, "put(%s%s)" % (self.hostkey, url_info.relurl))
            PriorityQueue.Queue._put(self, (depth, url_info))

    def msg(self, step):
        elapsed = time() - self.start_time
        kb = self.total_bytes / 1024.
        byte_rate = kb / elapsed
        hit_rate  = self.total_hits  / elapsed 
        template = "%s%.1f life, " + \
            "%d ready, %d open, %.2f KB ever, " + \
            "%s hits ever, %d(%d) succ(fail), %s KB/sec, " + \
            "%s hits/sec, next in %s" 
        msg = template % (step and "%s: " % step or "",
             elapsed, self.qsize(), len(self.conns), kb, 
             self.total_hits, self.succeeded, self.failed, 
             byte_rate, hit_rate, self.next_time() - time())
        syslog(LOG_DEBUG, msg)

    def robots_allows(self, relurl):
        if not self.rp: return True
        try:
            can_fetch = self.rp.can_fetch(
                self.fetcher.CRAWLER_NAME, 
                self.hostkey + relurl)
        except Exception, e:
            syslog("robotparser failed:  %s" % str(e))
            return True
        if not can_fetch: syslog("robots denies: %s" % relurl)
        return can_fetch

    def next_time(self):
        """project forward in time to when it is next polite to fetch"""
        if self.start_time is None:
            return 0
        return self.start_time + \
            max(self.total_bytes / self.fetcher.MAX_BYTE_RATE,
                self.total_hits  / self.fetcher.MAX_HITS_RATE)

    def update(self, url_info):
        """ update the hosts info for polite behavior """
        self.total_bytes += url_info.len_fetched_data
        self.total_hits  += 1
        self.msg("update")
        if url_info.relurl == "/robots.txt":
            syslog("%d B robots.txt" % url_info.len_fetched_data)
            self.robots_next = time() + self.fetcher.RECHECK_ROBOTS_INTERVAL
            self.rp = robotparser.RobotFileParser()
            try:
                self.rp.parse(url_info.raw_data)
            except Exception, e:
                syslog("robotparse failed: %s" % str(e))
                self.rp = None
            if self.rp:
                for pair in self:
                    (depth, url_info) = pair
                    if not self.robots_allows(url_info.relurl):
                        self.remove(pair)
                        url_info.state = ROBOTS_REJECTED
                        self.fetcher.out_proc(url_info)
        syslog(LOG_DEBUG, "out_proc(%s%s)" % (self.hostkey, url_info.relurl))
        self.fetcher.out_proc(url_info)
        # should detect if all of the URLs are redirecting to a
        # different host, and thus this host should get destroyed.

    def get_hostinfo(self):
        return HostInfo({
                "hostkey": self.hostkey,
                "total_hits": self.total_hits,
                "total_bytes": self.total_bytes,
                "start_time": self.start_time,
                "end_time": time(),
                "next_time": self.next_time(),
                })

class Fetcher(Process):
    """
The Fetcher class uses libcurl (via pycurl) for non-blocking,
streaming HTTP fetching.  

The Fetcher class implements polite rate limiting.  It uses a
PriorityQueue.Queue to hold Host objects in an idle state while it
waits for a polite time interval before fetching more.  The priority
in the PriorityQueue.Queue is a timestamp, and a future timestamp
means it is not yet polite to fetch more.  This allows the byte rate
and hit rate limiting to be enforced over the lifetime of the fetcher.
For periods of time shorter than the fetcher lifetime, the byte rate
and hit rate will burst to higher values while utilizing persistent
connections.  The PriorityQueue allows the fetcher to recover from
these burst by waiting a compensatory time period.  Longer-term
politeness requires state storage *outside* of the Fetcher class.

The input method for giving URLs to a Fetcher instance is through its
'packerQ' attribute, which is a multiprocessing.Queue filled with
instances of PyCrawler.URL.packer (see documentation for
PyCrawler.URL).

The Fetcher class has an outQ that contains URLinfo instances with the
results of a fetch and also HostInfo instances with host update
info.
    """
    ACCEPTED_SCHEMES = ('http',) # 'https', 'ftp', 'ftps', 'scp', 'sftp', 'tftp', 'telnet', 'dict', 'ldap', 'ldaps')

    FETCHER_TIMEOUT  = 600  # lifetime for all fetch operations, will force close slower operations
    CONNECT_TIMEOUT  = 20   # attempt connection timeout
    DOWNLOAD_TIMEOUT = 30   # download timeout, pycurl will end downloads that take longer
    MAX_CONNS = 128                    # num easy Curl objects to create
    MAX_CONNS_PER_HOST = 2             # num open connections allow per host
    MAX_HITS_RATE = 4 / 60.            # four per minute
    MAX_BYTE_RATE = 8 * 2**10          # 8 KB/sec
    MAX_STREAMED_REQUESTS = 100        # max number of streamed requests to allow before re-evaluating politeness
    MAX_FAILURES_PER_HOST = 5          # retires a host after this many
    FETCHES_TO_LIVE = None             # re-initializes all the libcurl objects after this many fetches
    RECHECK_ROBOTS_INTERVAL = 2 * DAYS

    # for robots.txt checking
    CRAWLER_NAME = "PyCrawler"
    CRAWLER_HOMEPAGE = "http://www.pycrawler.org/why-is-this-thing-hitting-my-website"

    SIMULATE = 0

    _debug = False

    def __init__(self, go=None, outQ=None, packerQ=None, params={}, **kwargs):
        """
        If 'go' is None, then it creates and sets a go Event.

        If 'outQ' is None, then it creates one.  Usually, this is the
        inQ to an AnalyzerChain.

        Use kwargs or params to set the class parameters.  If a
        parameter appears in both kwargs and params, kwargs wins.
        Takes a dict of (hostkey --> list of urls) and fetches them as
        fast as possible within politeness constraints.
        """
        # allow parameters to come as a dict or as kwargs
        params.update(kwargs)
        self.__dict__.update(params)
        self.USERAGENT = "Mozilla/5.0 (%s; +%s)" % \
            (self.CRAWLER_NAME, self.CRAWLER_HOMEPAGE)
        if not hasattr(self, "name"):
            self.name = self.CRAWLER_NAME
        Process.__init__(self, go, self._debug)
        syslog("Created with useragent: %s" % self.USERAGENT)
        self.outQ = outQ
        self.packerQ = packerQ
        self.pQ = PriorityQueue.Queue()
        self.m = None             # prep for first loop
        self.fetches = 0
        self.end_time = time() + self.FETCHER_TIMEOUT
        self.start_time = time()
        self.deltas = []
        self.arrivals = []
        self.idlelist = []
        self.freelist = []

    def init_curl(self):
        syslog("pycurl.global_init...")
        # This is not threadsafe.  See note above.
        pycurl.global_init(pycurl.GLOBAL_DEFAULT)
        syslog(str(pycurl.version_info()))
        syslog(repr(pycurl.version))
        # This is the one (and only) CurlMulti object that this
        # Fetcher instance will use, until (unless) we pass
        # FETCHES_TO_LIVE and dereference it in cleanup()
        syslog(LOG_DEBUG, "Creating CurlMulti object...")
        self.m = pycurl.CurlMulti()
        # Historically, there have been libcurl bugs associated with
        # pipelining.  It appears to work fine in v7.19.4.  Pipelining
        # using HTTP1.1 persistent connections with # keep-alive
        # handshaking.  
        self.m.setopt(pycurl.M_PIPELINING, 1)
        self.m.handles = []
        syslog(LOG_DEBUG, "Allocating %d Curl objects..." % self.MAX_CONNS)
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
                syslog("failed pycurl.Curl(): %s" % str(e))
                self.cleanup()
                #self.init_curl()  why would we call this here?
                break
            c.fp = None
            c.host = None
            c.url_info = None
            c.setopt(pycurl.FOLLOWLOCATION, 1)
            c.setopt(pycurl.MAXREDIRS, 5)
            c.setopt(pycurl.CONNECTTIMEOUT, self.CONNECT_TIMEOUT)
            c.setopt(pycurl.TIMEOUT,        self.DOWNLOAD_TIMEOUT)
            c.setopt(pycurl.NOSIGNAL, 1)
            c.setopt(pycurl.OPT_FILETIME, 1)
            c.setopt(pycurl.ENCODING, "")
            c.setopt(pycurl.DNS_CACHE_TIMEOUT, -1)
            c.setopt(pycurl.USERAGENT, self.USERAGENT)
            self.m.handles.append(c)
        self.freelist = self.m.handles[:]
        self.idlelist = []
        self.start_num_handles = 0
        self.fetches = 0

    def retire(self, host):
        """Put all url_info and host summary  in outQ"""
        syslog(LOG_DEBUG, "%s retiring without processing %d URLs" % 
               (host.hostkey, len(host)))
        while 1:
            try:
                p, u = host.get_nowait()
            except Queue.Empty:
                break
            self.out_proc(u)
        host_info = host.get_hostinfo()
        syslog("putting hostinfo: %s" % host_info)
        self.outQ_put(host_info)
        #del(host)  # nothing else should be referencing this host

    def out_proc(self, url_info):
        """
        Decrement the urls_in_flight counter before putting url_info
        into outQ.
        """
        self.urls_in_flight -= 1
        self.outQ_put(url_info)

    def outQ_put(self, yzable):
        """
        Put yzable in outQ and log while blocked.
        
        This blocks until the outQ accepts the yzable.
        """
        while self._go.is_set():
            try:
                self.outQ.put_nowait(yzable)
                return
            except Queue.Full:
                syslog(LOG_DEBUG, "outQ is FULL")
                sleep(1)
        syslog("Gave up on blocked outQ")

    def schedule(self, host):
        """
        Put host in priority queue, unless an instance is already in the pQ
        or if politeness projects next fetch beyond fetcher lifetime.
        """
        syslog(
            LOG_DEBUG, 
            "%s scheduling...:  scheduled=%s, len(conns)=%d" % \
                (host.hostkey, host.scheduled, len(host.conns)))
        if host.scheduled:
            return
        next = host.next_time()
        if next > self.end_time or len(host) == 0:
            self.retire(host)
            return
        syslog(
            LOG_DEBUG, 
            "%s --> pQ: %s in sec" % (host.hostkey, next - time()))
        self.pQ.put((next, host))
        host.scheduled = True

    def msg(self, step):
        msg = "%s%d / %d / %d (%d pending, next in %.0f sec), %d outQ, %d fetches" % (
            step and "%s: " % step or "",
            len(self.idlelist), 
            self.m and (len(self.m.handles) - len(self.freelist)) or 0,
            self.m and len(self.m.handles) or 0,
            self.pQ.qsize(), 
            self.pQ.top()[0] is not None and (self.pQ.top()[0] - time()) or -1,
            self.outQ and self.outQ.qsize() or 0,
            self.fetches)
        syslog(LOG_DEBUG, msg)

    def run(self):
        """
        This is the method that multiprocessing.Process executes in a
        separate process, so the various attributes (e.g. self.packer)
        have been pickled, sent over the wire, and separately
        instantiated here.  Thus, we do not change any state in
        self.packer here, and allow the calling process to handle all
        such updates by getting info out of outQ.
        """
        try:
            self.prepare_process()
            # if SIMULATE is other than zero, it means fake it for that
            # many seconds and then exit, used for testing
            # PyCrawler.Server
            if self.SIMULATE > 0:
                syslog(LOG_INFO, "simulating sleep(%d)" % self.SIMULATE)
                sleep(self.SIMULATE)
                return
            syslog(LOG_DEBUG, "Entering poll loop")
            try:
                packer = self.packerQ.get_nowait()
            except Queue.Empty:
                syslog("Got no packer")
                return
            # urls_in_flight includes one /robots.txt for each host
            self.urls_in_flight = len(packer) + len(packer.hosts)
            self.hosts = packer.dump()
            syslog("hosts: %s" % str(self.hosts))
            if self.urls_in_flight == 0:
                syslog(LOG_NOTICE, "ERROR: tried to run without any URLs, exiting.")
                return
            self.main()
        except Exception, exc:
            multi_syslog(exc)

    def main(self):
        """loop until no more in flight, or go has been cleared."""
        while self.urls_in_flight > 0 and self._go.is_set():
            self.msg("outer loop")
            if time() > self.end_time:
                break
            if self.FETCHES_TO_LIVE is not None and \
                    self.FETCHES_TO_LIVE < self.fetches:
                syslog(LOG_DEBUG, "past FETCHES_TO_LIVE, so purging hosts")
                self.cleanup()
            if self.m is None:
                # all hosts should be in pQ or retired, so
                # re-initialize all curl objects:
                self.init_curl()
            assert self.start_num_handles + len(self.freelist) + \
                len(self.idlelist) == len(self.m.handles), \
                "lost track of conns: %d + %d + %d != %d" % \
                (self.start_num_handles, len(self.freelist), 
                 len(self.idlelist), len(self.m.handles))
            if self.hosts:  syslog(LOG_DEBUG, "creating %d Host objects" % len(self.hosts))
            while self.hosts and self._go.is_set():
                (hostkey, relurls) = self.hosts.pop()
                host = Host(self, hostkey, relurls)
                self.pQ.put((host.next_time(), host))
            if len(self.pQ) == 0 and len(self.freelist) == len(self.m.handles):
                syslog(LOG_DEBUG, "pQ is empty, and all conns are free... done.")
                break
            # If there is a free curl object, try to get a host for it
            while self.freelist and self._go.is_set():
                self.msg("getif")
                (priority, host) = self.pQ.getif(maxP = time())
                if not host: break
                host.scheduled = False
                # enforce MAX_CONNS_PER_HOST
                while self.freelist and len(host.conns) < self.MAX_CONNS_PER_HOST:
                    c = self.freelist.pop()
                    c.host = host
                    c.host.conns.append(c)
                    c.stream_count = 0  # num requests that could have been pipelined
                    self.idlelist.append(c)
            while self.idlelist and self._go.is_set():
                url_info = None
                c = self.idlelist.pop()
                if c.host.failed >= self.MAX_FAILURES_PER_HOST \
                        and c.host.succeeded == 0:
                    syslog(LOG_INFO, c.host.hostkey + "too many failures, retiring.")
                    self.retire(c.host)
                elif c.stream_count >= self.MAX_STREAMED_REQUESTS:
                    syslog(LOG_INFO, c.host.hostkey + "streamed enough, scheduling for later")
                    self.schedule(c.host)
                else:
                    while url_info is None and self._go.is_set():
                        self.msg("host.get_nowait")
                        try:
                            priority, url_info = c.host.get_nowait()
                        except Queue.Empty:
                            if len(c.host.conns) == 1:  
                                # this is the last conn for this host, so retire:
                                self.retire(c.host)
                            url_info = None  # triggers freeing Curl object after break:
                            break
                        try:
                            c.setopt(pycurl.URL, c.host.hostkey + url_info.relurl)
                        except Exception, e:
                            syslog(LOG_NOTICE,
                                "%s failed c.setopt(pycurl.URL, %s) --> %s " % \
                                    (c.host.hostkey,
                                     c.host.hostkey + repr(url_info.relurl), 
                                     str(e)))
                            url_info.state = PYCURL_REJECTED
                            self.out_proc(url_info)
                            url_info = None
                            # loop again to try getting another
                            # url_info from this host
                if url_info is None:
                    syslog(LOG_DEBUG, "disconnecting %s" % c.host.hostkey)
                    c.host.conns.remove(c)
                    c.host = None
                    self.freelist.append(c)
                    continue  # loop again
                syslog(LOG_DEBUG, "%s popped: %s" % (c.host.hostkey, url_info.relurl))
                c.stream_count += 1
                c.url_info = url_info
                c.url_info.start = time()
                c.fp = StringIO()
                c.setopt(pycurl.WRITEFUNCTION, c.fp.write)
                self.m.add_handle(c)
                self.start_num_handles += 1
            # Run the internal curl state machine for the multi stack
            num_handles = self.start_num_handles
            while num_handles >= self.start_num_handles and self._go.is_set():
                syslog(LOG_DEBUG, "perform")
                ret, num_handles = self.m.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break
            #syslog("busted out of perform loop: num_handles(%d) start_num_handles(%d) _go.is_set()->%s" %
            #       (num_handles, self.start_num_handles, self._go.is_set()))
            # Check for curl objects which have terminated, and add them to the freelist
            while self._go.is_set():
                num_q, ok_list, err_list = self.m.info_read()
                syslog(LOG_DEBUG, "info_read: num_q(%d), ok_list(%d), err_list(%d)" % \
                           (num_q, len(ok_list), len(err_list)))
                finished_list = []
                for c in ok_list:
                    # replace hostkey, relurl with effurl if possible.
                    # This detects directory redirects, and puts a '/'
                    # on relurl for TextProcessing.get_links to
                    # function properly
                    effurl = c.getinfo(pycurl.EFFECTIVE_URL)
                    if effurl:
                        hostkey, relurl = URL.get_hostkey_relurl(effurl)
                        #syslog(LOG_DEBUG, "effurl %s --> %s" % (effurl, relurl))
                        if hostkey and relurl:
                            c.url_info.hostkey = hostkey
                            c.url_info.relurl  = relurl
                        else:
                            syslog(LOG_INFO, "This effurl did not make a hostkey and relurl: %s" % effurl)
                    # store download size (maybe was compressed)
                    c.url_info.len_fetched_data = c.getinfo(pycurl.SIZE_DOWNLOAD)
                    syslog(
                        LOG_NOTICE, "%d bytes: %s%s" % 
                        (c.url_info.len_fetched_data, c.url_info.hostkey, c.url_info.relurl))
                    c.url_info.raw_data = c.fp.getvalue()
                    c.url_info.state  = c.url_info.depth   # put the state back to being the depth
                    syslog(
                        LOG_DEBUG, 
                        "%s Success: %dB %s" % 
                        (c.host.hostkey, c.url_info.len_fetched_data, c.url_info.relurl))
                    c.host.succeeded += 1
                    finished_list.append(c)
                for c, errno, errmsg in err_list:
                    c.url_info.errno  = errno
                    c.url_info.errmsg = errmsg
                    c.url_info.state  = DEAD_LINK
                    syslog(LOG_DEBUG, "%s Failed: %s (%s) %s" % (c.host.hostkey, errmsg, errno, c.url_info.relurl))
                    c.host.failed += 1
                    finished_list.append(c)
                for c in finished_list:
                    c.url_info.end           = time()
                    c.url_info.http_response = c.getinfo(pycurl.RESPONSE_CODE)
                    c.url_info.last_modified = c.getinfo(pycurl.INFO_FILETIME)
                    c.host.update(c.url_info)
                    c.fp.close()
                    c.fp = None
                    c.url_info = None
                    self.m.remove_handle(c)  # when screwed up, this can segfault or hit a GIL
                    self.start_num_handles -= 1
                    self.fetches += 1  # decrementing lifetime toward init_curl
                    self.idlelist.append(c)  # maybe we can stream with this host
                if num_q == 0:
                    break
            # Currently no more I/O is pending, could do something in the meantime
            # (display a progress bar, etc.).
            # We just call select() to sleep until some more data is available.
            self.m.select(0.1)
            if len(self.freelist) == self.MAX_CONNS:
                if self.pQ.top()[0] is not None:
                    next = self.pQ.top()[0] - time() - 0.5
                    if next > 0:
                        syslog(LOG_DEBUG, "SLEEPING FOR %s SECONDS" % next)
                        sleep(next) # need way to break out of this on kill -2
        # broke out of the poll loop
        syslog(LOG_DEBUG, "out of poll loop")
        self.cleanup()
        syslog(LOG_DEBUG, "Exiting.")

    def cleanup(self):
        """
        Removes all host and URL state from the curl objects before
        dereferencing all of them so that the python garbage collector
        can get rid of them as it sees fit.
        """
        if self.m is not None:
            # rescue our url_info and hosts
            syslog("Cleanup after %d fetches" % self.fetches)
            for c in self.m.handles:
                if c.url_info is not None:
                    syslog(LOG_DEBUG, "cleanup: c.url_info --> None")
                    c.url_info.state = PENDING
                    c.url_info.raw_data = ""
                    c.url_info.len_fetched_data = 0
                    c.host.put((c.url_info.depth, c.url_info))
                    c.url_info = None
                if c.host is not None:
                    #syslog(LOG_DEBUG, "cleanup: c.host --> None, %s c.host.conns~%d " + \
                    #             "and c.host.qsize~%d" % \
                    #         (c.host.hostkey, len(c.host.conns), c.host.qsize()))
                    if len(c.host.conns) == 1:
                        # this is the last conn for this host, so stuff host into a Q:
                        syslog(
                            LOG_DEBUG, 
                            "cleanup: scheduling %s" % c.host.hostkey)
                        self.schedule(c.host)
                    syslog(LOG_DEBUG, "c.host.conns.remove(c)")
                    c.host.conns.remove(c)
                    syslog(LOG_DEBUG, "c.host = None")
                    c.host = None
        tot = 0
        for priority, host in self.pQ:
            tot += len(host)  
        syslog(LOG_DEBUG, "%d url_info dicts pending" % tot)
        # simply dereference all the curl objects and let the gc handle it
        self.m = None
        self.idlelist = []
        self.freelist = []
        try:
            # like pycurl.global_init, this is not threadsafe
            pycurl.global_cleanup()
        except:  # okay to fail
            pass
