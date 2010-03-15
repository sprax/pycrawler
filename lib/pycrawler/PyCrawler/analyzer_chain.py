#!/usr/bin/python2.6
"""
A document processing chain for PyCrawler.

"""
#$Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import Queue
import logging
import traceback
import multiprocessing
from time import time, sleep
from random import random
from PersistentQueue import Record

from process import Process, multi_syslog
from url import get_links, get_parts

class Analyzable(Record):
    """
    Base class for all data bundles passed through AnalyzerChain.

    The AnalyzerChain passes data bundles from Analyzer to Analzyer in
    the chain.  These data bundles must be subclasses of Analyzable.

    As a subclass of Record, this is picklable, so it can pass
    through multiprocessing.Queue and the like.  Also, the nameddict
    provides a single-line serialization scheme that enables the
    CrawlStateManager to store the two primary types of Analyzables in
    the disk-sort-ready PersistentQueue.
    """
    __slots__ = ()

    def __init__(self, *args, **kwargs):
        Record.__init__(self, *args, **kwargs)

class InvalidAnalyzer(Exception):
    pass

class FetchInfo(Analyzable):
    """
    >>> FetchInfo.create(url='http://foo')
    FetchInfo(raw_data=None, depth=None, start=None, end=None, state=None, links=[], hostkey='http://foo', relurl='/', last_modified=None)
    >>> FetchInfo.create(url='http://foo:7')
    FetchInfo(raw_data=None, depth=None, start=None, end=None, state=None, links=[], hostkey='http://foo:7', relurl='/', last_modified=None)
    """
    __slots__ = ('raw_data', 'depth', 'start', 'end', 'state', 'links',
                 'hostkey', 'relurl', 'last_modified')

    @classmethod
    def create(self, url=None, raw_data=None, depth=None, start=None, end=None,
               state=None, last_modified=None, links=[]):
        scheme, hostname, port, relurl = get_parts(url)
        hostkey = '%s://%s' % (scheme, hostname)
        if port:
            assert port[0] == ':'
            hostkey = hostkey + '%s' % port

        return FetchInfo(raw_data=raw_data, depth=depth, start=start,
                         end=end, state=state, last_modified=last_modified,
                         links=links, hostkey=hostkey, relurl=relurl)

class AnalyzerChain(Process):
    name = "AnalyzerChain"
    def __init__(self, go=None, debug=None):
        """
        Setup inQ and go Event
        """
        Process.__init__(self, go, debug)
        if debug:
            self.qlen = 10
        else:
            self.qlen = 100

        self.inQ  = multiprocessing.Queue(self.qlen)
        self._yzers = []
        self.in_flight = 0
        self.total_processed = 0

    def prepare_process(self):
        Process.prepare_process(self)
        self.logger = logging.getLogger('PyCrawler.AnalyzerChain.AnalyzerChain')

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
        if copies == 0:
            return
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
                self.logger.warning("run called with no analyzers")
                return
            self.logger.debug("starting yzers with queues between")

            queues = [multiprocessing.Queue(self.qlen)]
            for pos in range(len(self._yzers)):
                queues.append(multiprocessing.Queue(self.qlen))
                (yzer, copies) = self._yzers[pos]
                yzers = [yzer(queues[pos], queues[pos + 1], 
                              debug=self._debug)
                         for copy in range(copies)]
                for yzer in yzers:
                    yzer.start()
                self._yzers[pos] = yzers
            self.logger.debug("Starting main loop")
            yzable = None

            def pop_queue():
                try:
                    my_yzable = queues[-1].get_nowait()
                    self.in_flight -= 1
                    # delete each yzable as it exits the chain
                    try:
                        del(my_yzable)
                    except Exception, exc:
                        multi_syslog("failed to delete yzable: %s"\
                                         % traceback.format_exc(exc),
                                     logger=self.logger.warning)
                    self.total_processed += 1
                    return 1
                except Queue.Empty:
                    return 0

            last_in_flight = None
            last_in_flight_error_report = 0
            while self._go.is_set() or self.in_flight > 0:
                #syslog(
                #    LOG_DEBUG, "%d in_flight %d ever %d inQ.qsize %s" \
                #        % (in_flight, total_processed,
                #           self.inQ.qsize(),
                #           multiprocessing.active_children()))

                # We simply *must* always attempt to get objects from the queue if
                # they are available.  it is never acceptable to block on putting things
                # in the next queue, as there may be things in flight we need.
                pop_queue()

                try:
                    yzable = self.inQ.get_nowait()
                except Queue.Empty:
                    yzable = None
                if yzable is not None:
                    # We need to try to empty the queue as we put new items in,
                    # otherwise a deadlock is possible.
                    while self._go.is_set():
                        try:
                            queues[0].put_nowait(yzable)
                            # We must increment in_flight here, as if _go.is_set() hits,
                            # we would lose the in-flight packet.
                            self.in_flight += 1
                            last_in_flight = time()
                            break
                        except Queue.Full:
                            if not pop_queue():
                                sleep(0.5)

                # if none are in_flight, then we can sleep here
                curtime = time()
                if self.in_flight == 0:
                    sleep(1)
                elif curtime - last_in_flight > 60:
                    # report warnings if we've been waiting too long!
                    if curtime - last_in_flight_error_report > 60:
                        self.logger.warning('Most recent in-flight packet over %d seconds old, ' \
                                            '%d outstanding!' % (curtime - last_in_flight,
                                                                 self.in_flight))
                        last_in_flight_error_report = curtime

            # go is clear and none in_flight, stop all analyzers
            self.logger.info("Finished main loop")
            try:
                self.inQ.close()
                self.inQ.cancel_join_thread()
            except Exception, exc:
                multi_syslog(exc, logger=self.logger.warning)
            # stop all yzers
            for pos in range(len(self._yzers)):
                for yzer in self._yzers[pos]:
                    try:
                        yzer.stop()
                    except Exception, exc:
                        multi_syslog("%s(%d)" % (yzer.name, yzer.pid), exc,
                                     logger=self.logger.warning)
            self.logger.debug("stopped all Analyzers")
        except Exception, exc:
            multi_syslog(exc, logger=self.logger.warning)
        try:
            self.cleanup_process()
        except Exception, exc:
            multi_syslog(exc, logger=self.logger.warning)

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
    def __init__(self, inQ, outQ, debug=False, trace=False):
        """
        Sets up an inQ, outQ
        """
        Process.__init__(self, go=None, debug=debug)
        self.inQ  = inQ
        self.outQ = outQ
        self.trace = trace

    def run(self):
        """
        Gets yzable objects out of inQ, calls self.analyze(yzable)
        """
        try:
            self.prepare_process()
            self.prepare()
            self.logger.debug("Starting.")
            while self._go.is_set():
                try:
                    yzable = self.inQ.get_nowait()
                except Queue.Empty:
                    sleep(1)
                    continue
                self._trace("Analyzer %s getting ready to process %s" % (type(self).__name__,
                                                                         yzable))
                try:
                    yzable = self.analyze(yzable)
                except Exception, exc:
                    multi_syslog(
                        msg="analyze failed on: %s%s" % ( \
                            hasattr(yzable, "hostkey") and yzable.hostkey or "",
                            hasattr(yzable, "relurl") and yzable.relurl or ""),
                        exc=exc,
                        logger=self.logger.warning)
                # this blocks when processes later in the chain block
                initial_block = time()
                last_blocked = initial_block
                while self._go.is_set():
                    try:
                        self.outQ.put_nowait(yzable)
                        break
                    except Queue.Full:
                        cur = time()
                        if (cur - last_blocked) > 10:
                            self.logger.warning("Chain blocked for %d seconds" % (cur - initial_block))
                            last_blocked = cur
                        sleep(1)
                self._trace("Analyzer %s finished processing %s" % (type(self).__name__,
                                                                    yzable))

            self.cleanup()
        except Exception, exc:
            multi_syslog(exc, logger=self.logger.warning)

        try:
            self.cleanup_process()
        except Exception, exc:
            multi_syslog(exc, logger=self.logger.warning)

    def _trace(self, *args, **kwargs):
        """
        Log debug message if tracing turned on
        """
        if self.trace:
            self.logger.debug(*args, **kwargs)

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
        self.logger.info("Prepare.")
    def cleanup(self):
        """
        Called at the very end of the multiprocessing.Process.run
        function.
        """
        self.logger.info("Cleanup.")

class GetLinks(Analyzer):
    """
    Get links from page.

    >>> x=GetLinks(Queue.Queue(), Queue.Queue())
    >>> x.analyze("This is a string not a FetchInfo")
    'This is a string not a FetchInfo'
    """

    name = "GetLinks"
    def analyze(self, yzable):
        """uses URL.get_links to get URLs out of each page
        and pass it on as an attr of the yzable"""
        if isinstance(yzable, FetchInfo):
            #syslog(yzable.raw_data)
            errors, host_and_relurls_list = get_links(
                yzable.hostkey,  
                yzable.relurl, 
                yzable.raw_data, 
                yzable.depth)
            if errors:
                self.logger.debug(", ".join(["[%s]" % x for x in errors]))
            yzable.links = host_and_relurls_list
        return yzable

class LogInfo(Analyzer):
    name = "LogInfo"
    def analyze(self, yzable):
        self.logger.info("Analyze called with %s" % yzable)
        if isinstance(yzable, FetchInfo):
            self.logger.info("Analyze: %s%s" % (yzable.hostkey, yzable.relurl))
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
        self.logger.info("doing cleanup")
        out = ""
        out += "fetcher finished, now analyzing times\n"
        self.deltas.sort()
        out += "%d deltas to consider\n" % len(self.deltas)
        if not self.deltas: 
            self.logger.info("Apparently saw no URLinfo instances")
            return
        median = self.deltas[int(round(len(self.deltas)/2.))][0]
        mean = 0.
        for d in self.deltas:
            mean += d[0]
        mean /= float(len(self.deltas))
        success = 0
        for d in self.deltas:
            success += 1
        out += "%.4f mean, %.4f median, %.2f%% succeeded\n" % \
            (mean, median, 100. * success/float(len(self.deltas)))
        begin = 0
        self.logger.info("entering cleanup for loop")
        for i in range(1,11):                      # consider ten bins
            frac = i / 10.
            end = int(len(self.deltas) * frac)     # index number of the last item in this bin
            t = self.deltas[end-1][0]              # slowest time in this bin
            num_per_frac = len(self.deltas[begin:end])
            if num_per_frac == 0:
                num_per_frac = 1
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
        multi_syslog(out, logger=self.logger.debug)
        self.logger.info("SpeedDiagnostics finished")

