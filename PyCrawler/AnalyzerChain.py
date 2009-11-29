"""
A document processing chain for PyCrawler.

"""
#$Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import os
import sys
import URL
import Queue
import traceback
import multiprocessing
from time import time, sleep
from syslog import syslog, openlog, LOG_INFO, LOG_DEBUG, LOG_NOTICE, LOG_NDELAY, LOG_CONS, LOG_PID, LOG_LOCAL0
from random import random
from TextProcessing import get_links

URLinfo_type = "URLinfo"
HostSummary_type = "HostSummary_type"

class Analyzable:
    def __init__(self, yzable_type=None):
        self.type = yzable_type

class InvalidAnalyzer(Exception): pass

class AnalyzerChainPositionConflict(Exception): pass

class AnalyzerChain(multiprocessing.Process):
    name = "AnalyzerChain"
    def __init__(self, go=None):
        """
        Setup inQ and go Event
        """
        multiprocessing.Process.__init__(self, name=self.name)
        self.inQ  = multiprocessing.Queue(10)
        self.go = go or multiprocessing.Event()
        self.go.set()
        self.analyzers = {}
        self.in_flight = 0

    def add_analyzer(self, position, analyzer, copies=1):
        """
        analyzer should be a subclass of Analyzer.  The specified
        number of copies of analyzer will not be started while
        executing this method.  Rather, they will be started after the
        AnalyzerChain itself starts.  All of them will be placed at
        the specified position in this AnalyzerChain.

        Only one type of analyzer can occupy a given position.
        Setting copies to a number larger than one allows you to run
        multiple instances of the same yzer, and each job will pass
        through only one of them (at random).
        """
        if copies == 0: return
        if not hasattr(analyzer, "name"):
            raise InvalidAnalyzer("missing name attr")
        if position in self.analyzers:
            raise AnalyzerChainPositionConflict()
        # make a set of pointers to this analyzer class, but don't
        # instantiate them, that must happen in run
        yzers = [analyzer for count in range(copies)]
        # Put them in position in analyzer chain
        self.analyzers[position] = yzers

    def log(self, priority=LOG_DEBUG, msg=None, step=None):
        if msg is None: msg = self.msg(step)
        syslog(priority, msg)

    def msg(self, step=None):
        return "%s%d in_flight, %d ever, %d inQ.qsize, %s" % (
            step and "%s: " % step or "", self.in_flight, 
            self.total_processed, self.inQ.qsize(), 
            multiprocessing.active_children())

    def stop(self):
        """
        Causes the AnalyzerChain to stop after processing all
        in_flight requests.
        """
        self.log(LOG_NOTICE, "Executing clean stop")
        self.go.clear()

    def run(self):
        """
        Gets an Analyzable object from inQ and feeds them into the
        chain of analyzers, which are its child processes.
        """
        openlog(self.name, LOG_NDELAY|LOG_CONS|LOG_PID, LOG_LOCAL0)
        # Now prepare to run the chain
        self.in_flight = 0
        self.total_processed = 0
        self.log()
        positions = self.analyzers.keys()
        positions.sort()
        if not positions: 
            self.log(msg="run called without any analyzers")
            return
        # start all the analyzers as children
        for pos in self.analyzers:
            for i in range(len(self.analyzers[pos])):
                # instantiate and start instance of this analyzer
                yzer = self.analyzers[pos][i]()
                yzer.start()
                self.analyzers[pos][i] = yzer
        self.log()
        # Now run the chain
        self.log(msg="starting AnalyzerChain loop")
        try:
            while self.go.is_set() or self.in_flight > 0:
                self.log(step="outer loop")
                # list of Analyzable instances to carry through all
                # positions in the chain
                yzables_in  = []
                try:
                    yzable = self.inQ.get_nowait()
                    yzables_in.append(yzable)
                    self.in_flight += 1
                    self.total_processed += 1
                    self.log(msg="inQ gave yzable.type=" + yzable.type)
                except Queue.Empty:
                    pass
                for pos in positions:
                    self.log(step="for pos in positions")
                    yzers = self.analyzers[pos]
                    # make a list for holding all the yzables we get
                    # out of this group of yzers
                    yzables_out = self.pop_all(pos)
                    while len(yzables_in) > 0:
                        self.log(step="while len(yzables_in) > 0")
                        yzables_out += self.pop_all(pos)
                        # now put yzables into this group, starting at
                        # random position in the group
                        i = int(random() * len(yzers))
                        not_blocked = True
                        while len(yzables_in) > 0 and not_blocked:
                            self.log(step="inner not_blocked while len(yzables_in) > 0")
                            yzable = yzables_in.pop()
                            count = 0
                            while True:
                                self.log(step="inner inner one yzable")
                                try:
                                    yzers[i].inQ.put_nowait(yzable)
                                    count = 0
                                    break
                                except Queue.Full:
                                    self.log(step="skipping full inQ for %s (pid=%s)"%
                                             (yzer.name, yzer.pid))
                                    i = (i+1) % len(yzers)
                                    count += 1
                                if count > (2 * len(yzers)):
                                    # we have looped twice without
                                    # success, so assume we are
                                    # blocked.  Put yzable back in
                                    # list and break out so we can
                                    # attempt to get more yzables out
                                    # of yzers
                                    self.log(step="looped twice breaking out")
                                    yzables_in.append(yzable)
                                    not_blocked = False
                                    break
                        # if any yzables_in, then keep looping on this
                        # group, otherwise:
                    # move on to next pos in analyzers
                    yzables_in = yzables_out
                # done with all positions, ready to loop back to check
                # inQ.  But first, decrement in_flight by the number
                # we took off the last position in the chain.
                for yzable in yzables_out:
                    self.log(step="out gave yzable.type=" + yzable.type)
                    self.in_flight -= 1
                    # delete each yzable as it exits the chain
                    del(yzable)
                # if non are in_flight, then we can sleep here
                if self.in_flight == 0: sleep(1)
            # go is clear and none in_flight, stop all analyzers
            self.log(msg="finished AnalyzerChain loop")
            try:
                self.inQ.close()
                self.inQ.cancel_join_thread()
            except Exception, exc:
                self.log(msg=traceback.format_exc(exc))
            # stop and join all yzers
            for pos in positions:
                for yzer in self.analyzers[pos]:
                    try:
                        yzer.stop()
                    except Exception, exc:
                        self.log(msg="%s (%d): %s" % (yzer.name, yzer.pid, exc))
            self.log(msg="stopped all Analyzers in AnalyzerChain")
        except Exception, exc:
            self.log(msg="main loop: " + traceback.format_exc(exc))

    def pop_all(self, pos):
        "attempt to get yzables out of yzers at pos"
        yzables_out = []
        for yzer in self.analyzers[pos]:
            #if not yzer.is_alive(): do something?
            try:
                yzable = yzer.outQ.get_nowait()
                yzables_out.append(yzable)
            except Queue.Empty:
                pass
        return yzables_out

    def list_analyzers(self):
        """
        Returns a list of tuples (position, analyzer.name, copies)
        """
        ret = []
        for pos in self.analyzers:
            yzers = self.analyzers[pos]
            ret.append((pos, yzers[0].name, len(yzers)))
        return ret

    def remove_analyzer(self, position, name):
        """
        Remove all copies from position if they have the given name.
        This does not check whether there are jobs in process with
        these yzers.  You must check that first.
        """
        yzers = self.analyzers[pos]
        if yzers[0].name == name:
            del(self.analyzers[pos])
            for yzer in yzers:
                yzer.stop()
                yzer = None  # let gc cleanup

class Analyzer(multiprocessing.Process):
    """
    Super class for all content analyzers.  This creates Queues for
    passing document-processing jobs through a chain of Analyzer
    instances.

    Subclasses should implement .prepare() and .analyze(job) and
    .cleanup() and set the 'name' attr.
    """
    name = "Analyzer Base Class"
    def __init__(self):
        """
        Sets up an inQ, outQ
        """
        multiprocessing.Process.__init__(self, name=self.name)
        self.go = multiprocessing.Event()
        self.go.set()
        self.inQ  = multiprocessing.Queue(10)
        self.outQ = multiprocessing.Queue(10)

    def stop(self):
        """
        Causes the Analyzer to stop
        """
        self.log(LOG_NOTICE, "Executing clean stop")
        self.go.clear()

    def log(self, priority=LOG_DEBUG, msg=None, step=None):
        if msg is None: msg = self.msg(step)
        syslog(priority, "%s: %s" % (self.name, msg))

    def msg(self, step=None): 
        return "%d inQ.qsize" % self.inQ.qsize()

    def run(self):
        """
        Gets yzable objects out of inQ, calls self.analyze(yzable)
        """
        openlog(self.name, LOG_NDELAY|LOG_CONS|LOG_PID, LOG_LOCAL0)
        self.prepare()
        self.log(msg="starting")
        try:
            while self.go.is_set():
                self.log()
                try:
                    yzable = self.inQ.get_nowait()
                except Queue.Empty:
                    sleep(1)
                    continue
                try:
                    yzable = self.analyze(yzable)
                except Exception, exc:
                    self.log(LOG_NOTICE, "Encountered error while processing: %s%s --> %s" % (
                            yzable.hostkey, 
                            hasattr(yzable, "relurl") and yzable.relurl or "", 
                            traceback.format_exc(exc)))
                self.outQ.put(yzable)
        except Exception, exc:
            self.log(msg="while self.go.is_set() loop had: %s" % traceback.format_exc(exc))
        try:
            self.inQ.close()
            self.inQ.cancel_join_thread()
            self.outQ.close()
            self.outQ.cancel_join_thread()
        except Exception, exc:
            self.log(msg=traceback.format_exc(exc))
        self.cleanup()

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
        Create whatever special state or tables this analyzer needs in
        the database.  Do not modify the MultiFetcher's tables, called
        'urls' and 'hosts'.  You can refer to rows in the urls table
        using the md5 column, which is the md5 hash of the full url.
        """
        return ""
    def cleanup(self):
        """
        Called at the very end of the multiprocessing.Process.run
        function.
        """
        self.log(msg="finishing")

class DumpLinks(Analyzer):
    name = "DumpLinks"
    def prepare(self):
        "makes a packer for this instance of DumpLinks"
        self.packer = URL.packer()
    def analyze(self, yzable):
        """uses TextProcessing.get_links to get URLs out of each page
        and stores them in this GetLink instance's packer"""
        if yzable.type == URLinfo_type:
            #self.log(msg=yzable.raw_data)
            errors, host_and_relurls_list = get_links(
                yzable.hostkey,  
                yzable.relurl, 
                yzable.raw_data, 
                yzable.depth,
                ('http',))
            if errors: self.log(msg=", ".join(["[%s]" % x for x in errors]))
            errors = self.packer.expand(host_and_relurls_list)
            if errors: self.log(msg=", ".join(["[%s]" % x for x in errors]))
            yzable.links = host_and_relurls_list
            total = 0
            for hostkey, relurls in host_and_relurls_list:
                total += len(relurls)
            self.log(msg="Got %d urls" % total)
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
        if yzable.type == URLinfo_type:
            #self.log(msg=yzable.raw_data)
            errors, host_and_relurls_list = get_links(
                yzable.hostkey,  
                yzable.relurl, 
                yzable.raw_data, 
                yzable.depth)
            if errors: self.log(msg=", ".join(["[%s]" % x for x in errors]))
            yzable.links = host_and_relurls_list
        return yzable

class SaveCrawlState(Analyzer):
    name = "SaveCrawlState"
    def prepare(self):
        self.lines = 0
        self.output_path = "CrawlState.%d" % self.pid
        self.urls_fh = open(self.output_path + ".urls", "a")
        self.hosts_fh = open(self.output_path + ".hosts", "a")
    def analyze(self, yzable):
        if yzable.type == URLinfo_type:
            URL.write_URLinfo(yzable, self.urls_fh)
            self.log(msg="wrote lines for docid: " + yzable.docid)
        elif yzable.type == HostSummary_type:
            URL.write_HostSummary(yzable, self.hosts_fh)
            self.log(msg="wrote line for hostid: " + yzable.hostid)
        return yzable
    def cleanup(self):
        for fh in [self.urls_fh, self.hosts_fh]:
            fh.flush()
            fh.close()

class MakeContentData(Analyzer):
    name = "MakeContentData"
    def analyze(self, yzable):
        """uses TextProcessing.get_links to get URLs out of each page
        and stores them in this GetLink instance's packer"""
        if yzable.type == URLinfo_type:
            #self.log(msg=yzable.raw_data)
            errors, host_and_relurls_list = get_links(
                yzable.hostkey,  
                yzable.relurl, 
                yzable.raw_data, 
                yzable.depth)
            if errors: self.log(msg=", ".join(["[%s]" % x for x in errors]))
            errors = self.packer.expand(host_and_relurls_list)
            if errors: self.log(msg=", ".join(["[%s]" % x for x in errors]))
            yzable.links = host_and_relurls_list
            total = 0
            for hostkey, relurls in host_and_relurls_list:
                total += len(relurls)
            self.log(msg="Got %d urls" % total)
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
    def prepare(self):
        self.log(msg="Prepare.")
    def analyze(self, yzable):
        self.log(msg="Analyze called with %s" % yzable)
        if yzable.type == "URLinfo":
            self.log(msg="Analyze: %s%s" % (yzable.hostkey, yzable.relurl))
        return yzable
    def cleanup(self):
        self.log(msg="Cleanup.")

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
        #self.log(msg=str(yzable))
        if yzable.type == URLinfo_type:
            self.deltas.append((yzable.end - yzable.start, yzable.state))
            self.arrivals.append(time() - self.start_time)
            #self.log()
        return yzable
    def cleanup(self):
        """send a long message to the log"""
        stop = time()
        self.log(msg="doing cleanup")
        out = ""
        out += "fetcher finished, now analyzing times\n"
        self.deltas.sort()
        out += "%d deltas to consider\n" % len(self.deltas)
        if not self.deltas: 
            self.log(msg="Apparently saw no URLinfo instances")
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
        self.log(msg="entering cleanup for loop")
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
            out += "%.f%%\t%.4f sec\t%.4f per sec\t%d (%.2f%%) succeeded\t%d failed\n" % \
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
        self.log(msg=out)
        self.log(msg="SpeedDiagnostics finished")

