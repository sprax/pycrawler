"""
Tests for PyCrawler.AnalyzerChain
"""
#$Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import os
import sys
import traceback
from StringIO import StringIO

import logging
from logging import getLogger, StreamHandler

from PyCrawler import AnalyzerChain, Analyzer, FetchInfo, GetLinks, LogInfo, \
    analyzer_chain, SpeedDiagnostics

Analyzable = analyzer_chain.Analyzable

import multiprocessing
from time import sleep, time
from signal import signal, alarm, SIGALRM, SIGHUP, SIGINT, SIGQUIT, SIGABRT, SIGTERM, SIGPIPE, SIG_IGN

from nose.tools import raises

class AnalyzableString(Analyzable):
    __slots__ = ['str']

    def __str__(self):
        return self.str

class BrokenAnalyzer(Analyzer):
    name = "BrokenAnalyzer"
    def prepare(self):
        logging.debug("Prepare.")
    def analyze(self, yzable):
        logging.debug("Analyze called with %s" % yzable)
        raise Exception("failing intentionally for a test")
        return yzable
    def cleanup(self):
        logging.debug("Cleanup.")

#def test_broken_yzable_fini():
#    """
#    Test that analyzing an analyzable with a broken .__fini__
#    does the right thing.
#    """
#    # FIXME: implement test
#    pass

def test_no_analyzers():
    """
    Test to ensure that an AnalyzerChain with no analyzers immediately exits.
    """

    ac = AnalyzerChain(debug=True)
    ac.start()
    try:
        # Give a small amount of time for it to finish
        sleep(1)
        assert not multiprocessing.active_children()
        # FIXME: test logs
    except Exception:
        ac.stop()
        raise

def test_sleeping_analyzer_longqueue():
    """
    Test to make sure code that waits for in-flight analyzables works.
    """
    test_sleeping_analyzer(qlen=5)

def test_sleeping_analyzer(qlen=1, ):
    """
    Test to make sure queue stall code works.
    """
    start = time()

    analyzer_timeout = 2

    class SleepingAnalyzer(Analyzer):
        TIMEOUT = analyzer_timeout+2
        def analyzer(self, yzable):
            sleep(self.TIMEOUT)
            return yzable

    class LongSleepingAnalyzer(SleepingAnalyzer):
        TIMEOUT = analyzer_timeout+3

    # FIXME: test that this fires off warning log.

    # keep queue short to test stalls.
    ac = AnalyzerChain(debug=True, timewarn=analyzer_timeout/2.0,
                       timeout=analyzer_timeout, qlen=qlen,
                       queue_wait_sleep=0.01)

    # Have various sleep states to test queue stalls.
    ac.append(SleepingAnalyzer, 1)
    ac.append(LongSleepingAnalyzer, 1)
    ac.append(SleepingAnalyzer, 1)
    ac.start()

    try:
        ac.inQ.put(AnalyzableString('Analyzable string'))
        ac.inQ.put(AnalyzableString('Analyzable string'))

        ac.stop()

        for i in range(2 * analyzer_timeout):
            actives = multiprocessing.active_children()
            logging.info("waiting for children: %s" % actives)
            if len(actives) == 0:
                break
            sleep(0.5)
        else:
            raise Exception("Sleeping analyzer doesn't die!")

        # FIXME: test logs!!
    finally:
        for p in multiprocessing.active_children():
            try:
                p.terminate()
            except: pass

def test_speed_diagnostics():
    """ Ensure that SpeedDiagnostics properly diagnoses speed. """
    ac = AnalyzerChain(debug=True, queue_wait_sleep=0.01)
    ac.append(SpeedDiagnostics, 1)

    #logstring = StringIO()
    #loghandler = StreamHandler(logstring)
    #getLogger("PyCrawler").addHandler(loghandler)
    
    ac.start()

    try:
        u = FetchInfo.create(**{
            "url": "http://www.google.com",
            "depth":   0, 
            "last_modified": 0,
            "raw_data": "This is my raw data",
            "end": time() - 10,
            "start": time() - 20,
            "state": 0,
            }) 

        ac.inQ.put(u)

        u.end += 10
        u.start -= 10
        u.state = 3 # rejection
        ac.inQ.put(u)

        ac.stop()

        for i in range(5):
            actives = multiprocessing.active_children()
            logging.info("waiting for children: %s" % actives)
            if len(actives) == 0:
                break
            if ac.inQ.empty():
                break
            sleep(1)
        else:
            raise Exception("SpeedDiagnostics analyzer doesn't die!")
    finally:
        for p in multiprocessing.active_children():
            try:
                p.terminate()
            except: pass

    #loghandler.flush()
    #loglines = logstring.getvalue().split('\n')

    #for i in range(len(loglines)):
    #    if loglines[i].endswith('doing cleanup'):
    #        break
    #else:
    #    raise Exception("Didn't find SpeedDiagnostics log!\n%s" % loglines )

    ## FIXME: test for accurate statistics.

    #for i in range(i, len(loglines)):
    #    if loglines[i].endswith('SpeedDiagnostics finished'):
    #        break
    #else:
    #    raise Exception("Didn't find SpeedDiagnostics log!\n%s" % loglines)

def test_zero_copyes():
    """
    Verify that appending a non-analyzer to an analyzer chain produces the proper exception.
    """
    ac = AnalyzerChain(debug=True, queue_wait_sleep=0.01)
    ac.append(LogInfo, 0)
    assert ac._yzers == []

@raises(analyzer_chain.InvalidAnalyzer)
def test_non_analyzer():
    """
    Verify that appending a non-analyzer to an analyzer chain produces the proper exception.
    """
    ac = AnalyzerChain(debug=True, queue_wait_sleep=0.01)
    ac.append(None, 1)

def test_broken_analyzer():
    """
    Verify that an analyzer whose analyze() method raises an exception fails as expected.
    """
    test_analyzer(with_broken_analyzer=True)

def test_analyzer(with_broken_analyzer=False, timeout=10):
    """ Test that some basic analyzing works. """
    ac = AnalyzerChain(debug=True, queue_wait_sleep=0.01)

    def stop(a=None, b=None):
        logging.debug("received %s" % a)
        ac.stop()

    for sig in (SIGALRM, SIGHUP, SIGINT, SIGQUIT, SIGABRT, SIGTERM):
        signal(sig, stop)

    if with_broken_analyzer:
        logging.debug("adding a broken Analyzer")
        ac.append(BrokenAnalyzer, 3)
    ac.append(GetLinks, 10)
    ac.append(LogInfo, 1)

    hostkey = "http://www.cnn.com"
    text = "This is a test document." #urllib.urlopen(hostkey).read()
    u = FetchInfo.create(**{
            "url": hostkey + "/",
            "depth":   0, 
            "last_modified": 0,
            "raw_data": text
            }) 

    ac.start()
    try:

        ac.inQ.put(u)
        ac.stop()

        for i in range(timeout):
            actives = multiprocessing.active_children()
            logging.info("waiting for children: %s" % actives)
            if len(actives) == 0:
                break
            sleep(1)
        else:
            raise Exception("Failed after %d seconds" % timeout)
    finally:
        pass

if __name__ == "__main__":
    # run tests
    print "Starting tests"
    sys.stdout.flush()
    test()
    print "First test passed, now trying with_broken_analyzer"
    sys.stdout.flush()
    test(with_broken_analyzer=True)
    print "If you got here without any tracebacks or having to kill it, then the tests passed."
    sys.stdout.flush()
