"""
A document processing chain for PyCrawler.

"""
#$Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import URL
import Queue
import traceback
import multiprocessing
from time import time, sleep
from random import random
from syslog import syslog, LOG_DEBUG, LOG_NOTICE
from Process import Process, multi_syslog
from TextProcessing import get_links
from PersistentQueue import nameddict, SafeStr

class Analyzable(nameddict): 
    """
    Base class for all data bundles passed through AnalyzerChain.

    The AnalyzerChain passes data bundles from Analyzer to Analzyer in
    the chain.  These data bundles must be subclasses of Analyzable.

    As a subclass of nameddict, this is picklable, so it can pass
    through multiprocessing.Queue and the like.  Also, the nameddict
    provides a single-line serialization scheme that enables the
    CrawlStateManager to store the two primary types of Analyzables in
    the disk-sort-ready PersistentQueue.
    """
    pass

class FetchInfo(Analyzable):
    """
    This carries the primary information about a fetched document.  It
    stores this information in the nameddict structure of an
    Analyzable.
    """
    _defaults = {
        "links": [],
        "metadata": {},
        "hostkey": "",
        "relurl": "",
        "docid": "",
        "hostid": "",
        "hostbin": "",
        "depth": 0,
        "last_modified": 0,
        "http_response": None,
        "state": 0,
        "start": 0.,
        "end": 0.,
        "errno": 0,
        "errmsg": '',
        "len_fetched_data": 0,
        "raw_data": '',
        "content_data": None,
        "score": 0.,
        }

    # This establishes the order of the fields in instances of
    # PersistentQueue managed by CrawlStateManager
    _key_ordering = [
        "docid", "depth", "score", "last_modified", "http_response", 
        "relurl", "content_data"]

    _val_types = [
        str, int, float, int, int, 
        str, SafeStr]

    _sort_key = 0

    def __init__(self, attrs=None, url=None):
        """
        Sets up the Analyzable nature of FetchInfo.

        If provided, 'url' is parsed to get hostkey and relurl.

        This computes hostid, hostbin, and docid.
        """
        Analyzable.__init__(self, attrs)
        if url is not None:
            self.hostkey, self.relurl = URL.get_hostkey_relurl(url)
        if self.hostkey:
            self.set_external_attrs()

    def set_external_attrs(self):
        "computes properties that require the self.hostkey"
        assert bool(self.hostkey), "FetchInfo without hostkey! class = %s" % type(self)
        self.hostid, self.hostbin = URL.make_hostid_bin(self.hostkey)
        self.docid = URL.make_docid(self.hostkey, self.relurl)

    def make_FetchInfos_for_links(self):
        """
        Generates nearly-empty FetchInfo records for every outbound
        link in the page.
        """
        infos = []
        for hostkey, recs in self.links:
            for relurl, depth, last_modified, http_response, content_data in recs:
                infos.append(FetchInfo({
                            "hostkey": hostkey,
                            "relurl": relurl,
                            "depth": depth}))
        return infos

    @classmethod
    def accumulate(cls, acc_state, line):
        """
        De-duplicates link information by:

            summing scores (odds)

            maximizing depth (negative numbers mean farther from seed)

            maximizing last_modified, and keeping the most recent
            http_response and content_data
            
        """
        if line == "":
            # previous state was last record, so cause break
            return None, cls.dumps(acc_state)
        current = cls.loads(line)
        if acc_state is None:
            # first pass accumulation
            return current, None
        if current == acc_state:
            # same docid, so accumulate before returning
            acc_state.score += current.score
            acc_state.depth = max(
                acc_state.depth, 
                current.depth)
            if  acc_state.last_modified < current.last_modified:
                acc_state.last_modified = current.last_modified
                acc_state.http_response = current.http_response
                acc_state.content_data  = current.content_data
            return acc_state, None
        else:
            # new one! give back a serialized form as second value,
            # and 'current' becomes the acc_state:
            syslog(LOG_DEBUG, "accumulated a link for: %s" % cls.dumps(acc_state))
            syslog("FetchInfo I am? %s" % str(type(cls)))
            return current, cls.dumps(acc_state)

class InvalidAnalyzer(Exception): pass

class AnalyzerChain(Process):
    name = "AnalyzerChain"
    def __init__(self, go=None, debug=None):
        """
        Setup inQ and go Event
        """
        Process.__init__(self, go, debug)
        self.inQ  = multiprocessing.Queue(10)
        self._yzers = []

    def append(self, analyzer, copies=1):
        """
        analyzer should be a subclass of Analyzer.  The specified
        number of copies of analyzer will *not* be started while
        executing this method.  Rather, they will be started after the
        AnalyzerChain itself starts.  All of them will be placed in
        the same position at the current end of the AnalyzerChain.

        Only one type of analyzer can occupy a given position.
        Setting copies to a number larger than one allows you to run
        multiple instances of the same yzer, and each job will pass
        through only one of them (selected at random).
        """
        if copies == 0: return
        if not hasattr(analyzer, "name"):
            raise InvalidAnalyzer("missing name attr")
        self._yzers.append((analyzer, copies))

    def run(self):
        """
        Gets an Analyzable object from inQ and feeds them into the
        chain of analyzers, which are its child processes.
        """
        try:
            self.prepare_process()
            if not self._yzers:
                syslog(LOG_NOTICE, "run called with no analyzers")
                return
            syslog(LOG_DEBUG, "starting yzers with queues between")
            queues = [multiprocessing.Queue(10)]
            for pos in range(len(self._yzers)):
                queues.append(multiprocessing.Queue(10))
                (yzer, copies) = self._yzers[pos]
                yzers = [yzer(queues[pos], queues[pos + 1], 
                              debug=self._debug)
                         for copy in range(copies)]
                for yzer in yzers:
                    yzer.start()
                self._yzers[pos] = yzers
            syslog(LOG_DEBUG, "Starting main loop")
            yzable = None
            in_flight = 0
            total_processed = 0
            while self._go.is_set() or in_flight > 0:
                #syslog(
                #    LOG_DEBUG, "%d in_flight %d ever %d inQ.qsize %s" \
                #        % (in_flight, total_processed,
                #           self.inQ.qsize(),
                #           multiprocessing.active_children()))
                try:
                    yzable = self.inQ.get_nowait()
                    in_flight += 1
                except Queue.Empty:
                    yzable = None
                if yzable is not None:
                    # this can and should block
                    queues[0].put(yzable)
                try:
                    yzable = queues[-1].get_nowait()
                    in_flight -= 1
                    # delete each yzable as it exits the chain
                    try:
                        del(yzable)
                    except Exception, exc:
                        multi_syslog("failed to delete yzable: %s"\
                                         % traceback.format_exc(exc))
                    total_processed += 1
                except Queue.Empty:
                    pass
                # if non are in_flight, then we can sleep here
                if in_flight == 0: sleep(1)
            # go is clear and none in_flight, stop all analyzers
            syslog("Finished main loop")
            try:
                self.inQ.close()
                self.inQ.cancel_join_thread()
            except Exception, exc:
                multi_syslog(exc)
            # stop all yzers
            for pos in range(len(self._yzers)):
                for yzer in self._yzers[pos]:
                    try:
                        yzer.stop()
                    except Exception, exc:
                        multi_syslog("%s(%d)" % (yzer.name, yzer.pid), exc)
            syslog(LOG_DEBUG, "stopped all Analyzers")
        except Exception, exc:
            multi_syslog(exc)

    def list_analyzers(self):
        """
        Returns a list of tuples (analyzer.name, copies)
        """
        return [(copies[0].name, len(copies)) 
                for copies in self._yzers]

class Analyzer(Process):
    """
    Super class for all content analyzers.  This creates Queues for
    passing document-processing jobs through a chain of Analyzer
    instances.

    Subclasses should implement .prepare() and .analyze(job) and
    .cleanup() and set the 'name' attr.
    """
    name = "Analyzer Base Class"
    def __init__(self, inQ, outQ, debug):
        """
        Sets up an inQ, outQ
        """
        Process.__init__(self, go=None, debug=debug)
        self.inQ  = inQ
        self.outQ = outQ

    def run(self):
        """
        Gets yzable objects out of inQ, calls self.analyze(yzable)
        """
        try:
            self.prepare_process()
            self.prepare()
            syslog(LOG_DEBUG, "Starting.")
            while self._go.is_set():
                try:
                    yzable = self.inQ.get_nowait()
                except Queue.Empty:
                    sleep(1)
                    continue
                try:
                    yzable = self.analyze(yzable)
                except Exception, exc:
                    multi_syslog(
                        LOG_NOTICE,
                        msg="analyze failed on: %s%s" % ( \
                            hasattr(yzable, "hostkey") and yzable.hostkey or "",
                            hasattr(yzable, "relurl") and yzable.relurl or ""),
                        exc=exc)
                self.outQ.put(yzable)
            self.cleanup()
        except Exception, exc:
            multi_syslog(LOG_DEBUG, exc=exc)

    def analyze(self, yzable):
        """
        Gets a yzable as input and can do some data processing on
        it.  It *must* return the yzable, so it can continue does
        the AnalyzerChain.  Note that you  always get a yzable object as the 
        """
        return yzable
    def prepare(self):
        """
        Called after this multiprocessing.Process is started.
        Initialize whatever state you need for your self.analyze(job).
        Create whatever special state or tables this analyzer needs.
        """
        syslog("Prepare.")
    def cleanup(self):
        """
        Called at the very end of the multiprocessing.Process.run
        function.
        """
        syslog("Cleanup.")

class DumpLinks(Analyzer):
    name = "DumpLinks"
    def prepare(self):
        "makes a packer for this instance of DumpLinks"
        self.packer = URL.packer()
    def analyze(self, yzable):
        """uses TextProcessing.get_links to get URLs out of each page
        and stores them in this GetLink instance's packer"""
        if isinstance(yzable, FetchInfo):
            #syslog(yzable.raw_data)
            errors, host_and_relurls_list = get_links(
                yzable.hostkey,  
                yzable.relurl, 
                yzable.raw_data, 
                yzable.depth,
                ('http',))
            if errors: syslog(", ".join(["[%s]" % x for x in errors]))
            errors = self.packer.expand(host_and_relurls_list)
            if errors: syslog(", ".join(["[%s]" % x for x in errors]))
            yzable.links = host_and_relurls_list
            total = 0
            for hostkey, relurls in host_and_relurls_list:
                total += len(relurls)
            syslog("Got %d urls" % total)
        return yzable
    def cleanup(self):
        """
        Saves this DumpLinks instance's packer in a file
        """
        output_path = "DumpLinks.%d" % self.pid
        self.packer.dump_to_file(
            output_path, 
            make_file_name_unique=True,
           )

class GetLinks(Analyzer):
    name = "GetLinks"
    def analyze(self, yzable):
        """uses TextProcessing.get_links to get URLs out of each page
        and pass it on as an attr of the yzable"""
        if isinstance(yzable, FetchInfo):
            #syslog(yzable.raw_data)
            errors, host_and_relurls_list = get_links(
                yzable.hostkey,  
                yzable.relurl, 
                yzable.raw_data, 
                yzable.depth)
            if errors: syslog(LOG_DEBUG, ", ".join(["[%s]" % x for x in errors]))
            yzable.links = host_and_relurls_list
        return yzable

class MakeContentData(Analyzer):
    name = "MakeContentData"
    def analyze(self, yzable):
        """uses TextProcessing.get_links to get URLs out of each page
        and stores them in this GetLink instance's packer"""
        if isinstance(yzable, FetchInfo):
            #syslog(yzable.raw_data)
            errors, host_and_relurls_list = get_links(
                yzable.hostkey,  
                yzable.relurl, 
                yzable.raw_data, 
                yzable.depth)
            if errors: syslog(", ".join(["[%s]" % x for x in errors]))
            errors = self.packer.expand(host_and_relurls_list)
            if errors: syslog(", ".join(["[%s]" % x for x in errors]))
            yzable.links = host_and_relurls_list
            total = 0
            for hostkey, relurls in host_and_relurls_list:
                total += len(relurls)
            syslog("Got %d urls" % total)
        return yzable
    def cleanup(self):
        """
        Saves this GetLinks instance's packer in a file
        """
        output_path = "GetLinks.%d" % self.pid
        self.packer.dump_to_file(
            output_path, 
            make_file_name_unique=True,
            )

class LogInfo(Analyzer):
    name = "LogInfo"
    def analyze(self, yzable):
        syslog("Analyze called with %s" % yzable)
        if isinstance(yzable, FetchInfo):
            syslog("Analyze: %s%s" % (yzable.hostkey, yzable.relurl))
        return yzable

class SpeedDiagnostics(Analyzer):
    name = "SpeedDiagnostics"
    def prepare(self):
        """Initialize the two lists of information that we store for
        calculating speed info, and store the start time."""
        self.deltas = []
        self.arrivals = []
        self.start_time = time()
    def analyze(self, yzable):
        """for URLinfo objects, store the key timing and success/failure info"""
        #syslog(str(yzable))
        if isinstance(yzable, FetchInfo):
            self.deltas.append((yzable.end - yzable.start, yzable.state))
            self.arrivals.append(time() - self.start_time)
        return yzable
    def cleanup(self):
        """send a long message to the log"""
        stop = time()
        syslog("doing cleanup")
        out = ""
        out += "fetcher finished, now analyzing times\n"
        self.deltas.sort()
        out += "%d deltas to consider\n" % len(self.deltas)
        if not self.deltas: 
            syslog("Apparently saw no URLinfo instances")
            return
        median = self.deltas[int(round(len(self.deltas)/2.))][0]
        mean = 0.
        for d in self.deltas: mean += d[0]
        mean /= float(len(self.deltas))
        success = 0
        for d in self.deltas: success += 1
        out += "%.4f mean, %.4f median, %.2f%% succeeded\n" % \
            (mean, median, 100. * success/float(len(self.deltas)))
        begin = 0
        syslog("entering cleanup for loop")
        for i in range(1,11):                      # consider ten bins
            frac = i / 10.
            end = int(len(self.deltas) * frac)     # index number of the last item in this bin
            t = self.deltas[end-1][0]              # slowest time in this bin
            num_per_frac = len(self.deltas[begin:end])
            if num_per_frac == 0: num_per_frac = 1
            rate = t > 0 and num_per_frac / t or 0 # slowest rate in this bin
            success = 0
            failure = 0
            for d in self.deltas[begin:end]:
                if d[1] <= 1:  # means reject
                    success += 1
                else:          # means rejected for some reason
                    failure += 1
            begin = end        # bottom index of the next bin
            out += "%.f%%    %.4f sec    %.4f per sec    %d (%.2f%%) succeeded    %d failed\n" % \
                  (frac * 100, t, rate, success, 100.*success/num_per_frac, failure)
        # arrival time processing
        #arrivals.sort()
        window_sizes = [1., 10., 100., 1000.] # seconds
        binned_arrivals = {}
        for win in window_sizes:
            binned_arrivals[win] = {}
            max = int(win * (1 + int((stop-self.start_time)/win)))
            for i in range(0, max, int(win)):
                binned_arrivals[win][i] = 0
            #out += binned_arrivals[win] + "\n"
            for a in self.arrivals:
                k = int(win * int(a / win))
                binned_arrivals[win][k] += 1
            #out += binned_arrivals[win] + "\n"
            avg_rate = 0.
            for k in binned_arrivals[win]:
                avg_rate += binned_arrivals[win][k]/win
            avg_rate /= len(binned_arrivals[win])
            out += "Averaging over fixed bins of size %d, we have a rate of %.4f completions/second\n" % (win, avg_rate)
            bins = binned_arrivals[win].items()
            bins.sort()
            tot = 0.
            for bin, count in bins:
                tot += count
            for bin, count in bins:
                out += "%s --> %s, %.1f\n" % (bin, count, count / tot)
        multi_syslog(LOG_NOTICE, out)
        syslog("SpeedDiagnostics finished")

