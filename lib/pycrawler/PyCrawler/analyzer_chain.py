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
from ctypes import sizeof  # For checking overflow of multiprocessing.Value
import logging
import traceback
import multiprocessing
from time import time, sleep
from random import random
from PersistentQueue import Record, b64, JSON, RecordFIFO

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

class InvalidAnalyzer(Exception):
    pass

FetchInfo_template = (str, str, int, int, int, int, JSON, int, int, JSON)
class FetchInfo(Analyzable):
    """
    >>> FetchInfo.create(url='http://foo')
    FetchInfo(hostkey='http://foo', relurl='/', depth=None, start=None, end=None, state=None, links=[], last_modified=None, http_response=None)
    >>> FetchInfo.create(url='http://foo:7')
    FetchInfo(hostkey='http://foo:7', relurl='/', depth=None, start=None, end=None, state=None, links=[], last_modified=None, http_response=None)
    """
    __slots__ = ('hostkey', 'relurl', 'depth', 'start', 'end', 'state', 'links',
                 'last_modified', 'http_response', 'data')

    @classmethod
    def create(self, url=None, raw_data=None, depth=None, start=None, end=None,
               state=None, last_modified=None, links=[]):
        scheme, hostname, port, relurl = get_parts(url)
        hostkey = '%s://%s' % (scheme, hostname)
        if port:
            assert port[0] == ':'
            hostkey = hostkey + '%s' % port

        return FetchInfo(data={'raw_data': raw_data}, depth=depth, start=start,
                         end=end, state=state, last_modified=last_modified,
                         links=links, hostkey=hostkey, relurl=relurl,
                         http_response=None)

    def __repr__(self):
        args = ', '.join('%s=%r' % (k, v) for k, v in self._items() if k != 'data')
        return '%s(%s)' % (type(self).__name__, args)

class FetchInfoFIFO(RecordFIFO):
    """ Convenience class for a FIFO of FetchInfo. """
    def __init__(self, data_path, **kwargs):
        super(FetchInfoFIFO, self).__init__(record_class=FetchInfo,
                                            template=FetchInfo_template,
                                            data_path=data_path,
                                            **kwargs)

class AnalyzerChain(Process):
    name = "AnalyzerChain"
    def __init__(self, go=None, debug=None, qlen=100, timewarn=60,
                 timeout=None, queue_wait_sleep=0.5):
        """
        Setup inQ and go Event
        """
        Process.__init__(self, go, debug)

        self.qlen = qlen
        self.inQ  = multiprocessing.Queue(self.qlen)
        self._yzers = []
        self.in_flight = multiprocessing.Value('i', 0)
        self.total_processed = 0
        self.timeout = timeout
        self.timewarn = timewarn
        self.queue_wait_sleep = queue_wait_sleep
        self.last_in_flight = 0

        assert float(queue_wait_sleep) > 0

    def bored(self):
        self.in_flight.acquire()
        val = self.in_flight.value and self.inQ.empty()
        self.in_flight.release()
        return val

    def prepare_process(self):
        super(AnalyzerChain, self).prepare_process()
        self.logger = logging.getLogger('PyCrawler.AnalyzerChain.AnalyzerChain')

    def append(self, analyzer, copies=1, args=[], kwargs={}):
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
        self._yzers.append((analyzer, copies, args, kwargs))

    def enqueue_yzable(self, queue, yzable):
        """
        Try to enqueue item, emptying items that are finished analyzing if the queue
        is full so that it get room again.
        """

        # We need to try to empty the queue as we put new items in,
        # otherwise a deadlock is possible.
        while not self._stop.is_set():
            try:
                queue.put_nowait(yzable)
                # We must increment in_flight here, rather than
                # above, as if _go.is_set() hits,
                # we would lose the in-flight packet.
                self.in_flight.acquire()
                try:
                    # Ensure the integer will not overflow.
                    assert self.in_flight.value < (2**(sizeof(self.in_flight.get_obj())*8 - 1) - 1)
                    self.in_flight.value += 1
                finally:
                    self.in_flight.release()

                self.last_in_flight = time()
                break
            except Queue.Full:
                if not pop_queue():
                    sleep(self.queue_wait_sleep)

    def run(self):
        """
        Gets an Analyzable object from inQ and feeds them into the
        chain of analyzers, which are its child processes.
        """
        self.prepare_process()
        try:
            if not self._yzers:
                self.logger.warning("run called with no analyzers")
                # The next lines stops coverage, so doesn't get recorded as covered.
                return self.cleanup_process() # pragma: no cover

            self.logger.debug("starting yzers with queues between")

            queues = [multiprocessing.Queue(self.qlen)]
            for pos in range(len(self._yzers)):
                queues.append(multiprocessing.Queue(self.qlen))
                (yzer, copies, args, kwargs) = self._yzers[pos]
                yzers = [yzer(queues[pos], queues[pos + 1], 
                              *args,
                              debug=self._debug,
                              queue_wait_sleep=self.queue_wait_sleep,
                              in_flight=self.in_flight,
                              **kwargs
                              )
                         for copy in range(copies)]
                for yzer in yzers:
                    yzer.start()
                self._yzers[pos] = yzers
            self.logger.debug("Starting main loop")
            yzable = None

            def pop_queue():
                try:
                    # IMPORTANT! we acquire the lock FIRST to ensure that
                    # bored() works without races.
                    self.in_flight.acquire()

                    try:
                        my_yzable = queues[-1].get_nowait()
                        self.in_flight.value -= 1
                    finally:
                        self.in_flight.release()

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

            self.last_in_flight = None
            last_in_flight_error_report = 0
            while not self._stop.is_set() or self.in_flight.value > 0:
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
                    self.enqueue_yzable(queues[0], yzable)

                # if none are in_flight, then we can sleep here
                curtime = time()
                # FIXME: we may not want to sleep if things are in-flight
                # and we have successfully popped an item.  But we want to
                # ensure we don't spin here.
                sleep(self.queue_wait_sleep)
                if self.in_flight.value > 0 and self.timewarn and \
                        curtime - self.last_in_flight > self.timewarn:
                    # report warnings if we've been waiting too long!
                    if curtime - last_in_flight_error_report > self.timewarn:
                        self.logger.warning('Most recent in-flight packet over %d seconds old, ' \
                                            '%d outstanding!' % (curtime - self.last_in_flight,
                                                                 self.in_flight.value))
                        last_in_flight_error_report = curtime

                if self.in_flight.value != 0 and not self._stop.is_set() and \
                        self.timeout and (curtime - self.last_in_flight) > self.timeout:
                    break

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
    def __init__(self, inQ, outQ, debug=False, trace=False,
                 in_flight=None, queue_wait_sleep=1):
        """
        Sets up an inQ, outQ
        """
        Process.__init__(self, go=None, debug=debug)
        self.inQ  = inQ
        self.outQ = outQ
        self.trace = trace
        self.queue_wait_sleep = queue_wait_sleep
        self.in_flight = in_flight

        assert float(queue_wait_sleep) > 0

        if in_flight is not None:
            assert hasattr(in_flight, 'value')
            assert hasattr(in_flight, 'acquire')
            assert hasattr(in_flight, 'release')

    def prepare_process(self):
        super(Analyzer, self).prepare_process()
        self.logger = logging.getLogger("PyCrawler.Analyzer")

    def run(self):
        """
        Gets yzable objects out of inQ, calls self.analyze(yzable)
        """
        try:
            self.prepare_process()
            self.prepare()
            self.logger.debug("Starting.")
            while not self._stop.is_set():
                try:
                    yzable = self.inQ.get_nowait()
                except Queue.Empty:
                    sleep(self.queue_wait_sleep)
                    continue
                self._trace("Analyzer %s getting ready to process %s" % (type(self).__name__,
                                                                         yzable))

                # We need to keep things sane.  This is anti-duck-typing to be sure
                # we're sending things we intended to send.
                if not isinstance(yzable, Analyzable):
                    self.logger.error('Expected instance of Analyzable(), got %r' % yzable)
                    yzable = None

                try:
                    if yzable is not None:
                        yzable = self.analyze(yzable)
                except Exception, exc:
                    multi_syslog(
                        msg="analyze failed on: %s%s" % ( \
                            hasattr(yzable, "hostkey") and yzable.hostkey or "",
                            hasattr(yzable, "relurl") and yzable.relurl or ""),
                        exc=exc,
                        logger=self.logger.warning)

                # Drop yzable on request of analyzer.
                if yzable is None:
                    if self.in_flight is not None:
                        self.in_flight.acquire()
                        self.in_flight.value -= 1
                        self.in_flight.release()

                    self.logger.debug("Dropped yzable.")
                    continue

                # this blocks when processes later in the chain block
                initial_block = time()
                last_blocked = initial_block
                while not self._stop.is_set():
                    try:
                        self.outQ.put_nowait(yzable)
                        break
                    except Queue.Full:
                        cur = time()
                        if (cur - last_blocked) > 10:
                            self.logger.warning("Chain blocked for %d seconds" % (cur - initial_block))
                            last_blocked = cur
                        sleep(self.queue_wait_sleep)
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
        self.logger.info("Prepare %s." % self.name)
    def cleanup(self):
        """
        Called at the very end of the multiprocessing.Process.run
        function.
        """
        self.logger.info("Cleanup %s." % self.name)

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
        if isinstance(yzable, FetchInfo) and "raw_data" in yzable.data:
            errors, host_and_relurls_list = get_links(
                yzable.hostkey,  
                yzable.relurl, 
                yzable.data["raw_data"],
                yzable.depth)
            if errors:
                self.logger.debug(", ".join(["[%s]" % x for x in errors]))
            yzable.links = host_and_relurls_list
        return yzable

class LogInfo(Analyzer):
    name = "LogInfo"
    def analyze(self, yzable):
        self.logger.info("Analyze: %r" % yzable)
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
            if yzable.end is not None and yzable.start is not None:
                self.deltas.append((yzable.end - yzable.start, yzable.state))
            else:
                self.deltas.append((0, yzable.state))
            self.arrivals.append(time() - self.start_time)
        return yzable

    def stats(self):
        self.deltas.sort()
        if not self.deltas: 
            self.logger.info("Apparently saw no URLinfo instances")
            return None, None, None
        median = self.deltas[int(len(self.deltas)/2.)][0]
        mean = 0.
        for d in self.deltas:
            mean += d[0]
        mean /= float(len(self.deltas))
        success = 0
        for d in self.deltas:
            success += 1

        return mean, median, success

    def cleanup(self):
        """send a long message to the log"""
        stop = time()
        self.logger.info("doing cleanup")
        out = ""
        out += "fetcher finished, now analyzing times\n"
        out += "%d deltas to consider\n" % len(self.deltas)

        mean, median, success = self.stats()
        if success is None:
            return

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
