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

from PyCrawler import AnalyzerChain, Analyzer, GetLinks, LogInfo

import multiprocessing
from time import sleep, time
from syslog import syslog, openlog, LOG_INFO, LOG_DEBUG, LOG_NOTICE, LOG_NDELAY, LOG_CONS, LOG_PID, LOG_LOCAL0
from signal import signal, alarm, SIGALRM, SIGHUP, SIGINT, SIGQUIT, SIGABRT, SIGTERM, SIGPIPE, SIG_IGN


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

def test(with_broken_analyzer=False):

    openlog("AnalyzerChainTest", LOG_NDELAY|LOG_CONS|LOG_PID, LOG_LOCAL0)

    syslog(LOG_DEBUG, "making an AnalyzerChain")
    ac = AnalyzerChain.AnalyzerChain(debug=True)

    def stop(a=None, b=None):
        syslog(LOG_DEBUG, "received %s" % a)
        ac.stop()

    for sig in (SIGALRM, SIGHUP, SIGINT, SIGQUIT, SIGABRT, SIGTERM):
        signal(sig, stop)

    syslog(LOG_DEBUG, "setting alarm")
    alarm(10)

    if with_broken_analyzer:
        syslog(LOG_DEBUG, "adding a broken Analyzer")
        ac.append(BrokenAnalyzer, 3)
    syslog(LOG_DEBUG, "adding Analyzers")
    ac.append(AnalyzerChain.GetLinks, 10)
    ac.append(AnalyzerChain.LogInfo, 1)

    syslog(LOG_DEBUG, "Making FetchInfo")
    hostkey = "http://www.cnn.com"
    text = "This is a test document." #urllib.urlopen(hostkey).read()
    u = AnalyzerChain.FetchInfo(attrs={
            "hostkey": hostkey, 
            "relurl":  "/", 
            "depth":   0, 
            "last_modified": 0,
            "raw_data": text
            }) 
    syslog("constructed an FetchInfo with  str:" + str(u))

    syslog(LOG_DEBUG, "calling start")
    ac.start()
    syslog(LOG_DEBUG, "start returned")

    syslog(LOG_DEBUG, "putting a FetchInfo into the chain")
    ac.inQ.put(u)

    while True:
        actives = multiprocessing.active_children()
        syslog(LOG_DEBUG,  "waiting for children: %s" % actives)
        if len(actives) == 0: break
        sleep(1)

    syslog(LOG_DEBUG, "Test finished")

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
