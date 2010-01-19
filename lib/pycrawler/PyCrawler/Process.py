#!/usr/bin/python2.6
"""
A set of conveniences around multiprocessing.Process and syslog
"""
#$Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import traceback
import multiprocessing
from syslog import syslog, openlog, setlogmask, LOG_UPTO, LOG_INFO, LOG_DEBUG, LOG_NOTICE, LOG_NDELAY, LOG_CONS, LOG_PID, LOG_LOCAL0
from signal import signal, SIG_IGN, SIGINT, SIGHUP, SIGTERM, SIGQUIT

class Process(multiprocessing.Process):
    """
    Provides the minimal components of a multiprocessing.Process as
    used throughout PyCrawler.  These include a go event and syslog
    usage.
    """
    _debug = False

    def __init__(self, go=None, debug=None):
        """
        Keep self.name as its previously set value, or replace it with
        "Unnamed"

        If go is not provided, then create a self._go event, which
        self.prepare_process will set.  self.stop() clears self._go.
        """
        if debug is not None:
            self._debug = debug
        if not hasattr(self, "name"):
            self.name = "Unnamed"
        multiprocessing.Process.__init__(self, name=self.name)
        self._go = go or multiprocessing.Event()

    def prepare_process(self):
        """
        Set the go Event and open the syslog
        """
        for sig in [SIGINT, SIGHUP, SIGTERM, SIGQUIT]:
            signal(sig, SIG_IGN)
        self._go.set()
        openlog(self.name, LOG_NDELAY|LOG_CONS|LOG_PID, LOG_LOCAL0)
        if not self._debug:
            setlogmask(LOG_UPTO(LOG_INFO))

    def stop(self):
        syslog(LOG_DEBUG, "Stop called.")
        self._go.clear()

def multi_syslog(level=LOG_DEBUG, msg=None, exc=None):
    """
    A convenience function for sending multiple lines of information
    to the system log.  The lines can get intermingled with other
    messages, but it is still more readable that a single massive line
    for a traceback.

    Default for 'level' is LOG_DEBUG.

    If a single argument is passed, then it can be a string or an
    Exception.  If Exception, 'level' is set to LOG_NOTICE.  If
    string, 'level' is set to LOG_DEBUG.
    """
    if msg is None and exc is None:
        if isinstance(level, Exception):
            exc = level
            level = LOG_NOTICE
        elif type(level) is str:
            msg = level
            level = LOG_DEBUG
        else:
            msg = "multi_syslog single argument neither str nor Exception: %s" % repr(level)
    if exc is None and isinstance(msg, Exception):
        msg = traceback.format_exc(exc)
    if msg is None:
        msg = ""
    if exc is not None:
        msg += "\n" + traceback.format_exc(exc)
    rows = msg.splitlines()
    def log(row):
        syslog(level, row)
    map(log, rows)
