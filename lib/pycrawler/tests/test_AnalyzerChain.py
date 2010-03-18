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

from PyCrawler import AnalyzerChain, Analyzer, FetchInfo, GetLinks, LogInfo, analyzer_chain, \
     SpeedDiagnostics

import multiprocessing
from time import sleep, time
from syslog import syslog, openlog, LOG_INFO, LOG_DEBUG, LOG_NOTICE, LOG_NDELAY, LOG_CONS, LOG_PID, LOG_LOCAL0
from signal import signal, alarm, SIGALRM, SIGHUP, SIGINT, SIGQUIT, SIGABRT, SIGTERM, SIGPIPE, SIG_IGN

from nose.tools import raises

class BrokenAnalyzer(Analyzer):
    name = "BrokenAnalyzer"
    def prepare(self):
        syslog("Prepare.")
    def analyze(self, yzable):
        syslog("Analyze called with %s" % yzable)
        raise Exception("failing intentionally for a test")
        return yzable
    def cleanup(self):
        syslog("Cleanup.")

#def test_broken_yzable_fini():
#    """
#    Test that analyzing an analyzable with a broken .__fini__
#    does the right thing.
#    """
#    # FIXME: implement test
#    pass

def test_sleeping_analyzer():
    timeout = 1
    class SleepingAnalyzer(Analyzer):
        def analyzer(self, yzable):
            sleep(timeout + 2)
            return yzable
    # FIXME: test that this fires off warning log.
    ac = AnalyzerChain(debug=True, timeout=timeout, queue_wait_sleep=0.01)
    ac.append(SleepingAnalyzer, 1)
    ac.start()

    try:
        ac.inQ.put('Analyzable string')

        sleep(timeout)

        for i in range(10):
            actives = multiprocessing.active_children()
            logging.info("waiting for children: %s" % actives)
            if len(actives) == 0:
                break
            if ac.inQ.empty():
                ac.stop()
            sleep(0.5)
        else:
            raise Exception("Sleeping analyzer doesn't die!")

        # FIXME: test logs!!
    finally:
        ac.stop()
        while multiprocessing.active_children():
            sleep(0.1)

def test_speed_diagnostics():
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
            }) 

        ac.inQ.put(u)

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
        ac.stop()
        while multiprocessing.active_children():
            sleep(0.1)

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
    openlog("AnalyzerChainTest", LOG_NDELAY|LOG_CONS|LOG_PID, LOG_LOCAL0)

    syslog(LOG_DEBUG, "making an AnalyzerChain")
    ac = AnalyzerChain(debug=True, queue_wait_sleep=0.01)

    def stop(a=None, b=None):
        syslog(LOG_DEBUG, "received %s" % a)
        ac.stop()

    for sig in (SIGALRM, SIGHUP, SIGINT, SIGQUIT, SIGABRT, SIGTERM):
        signal(sig, stop)

    if with_broken_analyzer:
        syslog(LOG_DEBUG, "adding a broken Analyzer")
        ac.append(BrokenAnalyzer, 3)
    syslog(LOG_DEBUG, "adding Analyzers")
    ac.append(GetLinks, 10)
    ac.append(LogInfo, 1)

    syslog(LOG_DEBUG, "Making FetchInfo")
    hostkey = "http://www.cnn.com"
    text = "This is a test document." #urllib.urlopen(hostkey).read()
    u = FetchInfo.create(**{
            "url": hostkey + "/",
            "depth":   0, 
            "last_modified": 0,
            "raw_data": text
            }) 

    syslog(LOG_DEBUG, "calling start")
    ac.start()
    try:

        syslog(LOG_DEBUG, "start returned")

        syslog(LOG_DEBUG, "putting a FetchInfo into the chain")
        ac.inQ.put(u)

        for i in range(timeout):
            actives = multiprocessing.active_children()
            logging.info("waiting for children: %s" % actives)
            if len(actives) == 0:
                break
            if ac.inQ.empty():
                ac.stop()
            sleep(1)
        else:
            raise Exception("Failed after %d seconds" % timeout)
    finally:
        ac.stop()
        while multiprocessing.active_children():
            sleep(0.1)

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
