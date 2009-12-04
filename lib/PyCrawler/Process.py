#$Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import multiprocessing
from syslog import syslog, openlog, setlogmask, LOG_UPTO, LOG_INFO, LOG_DEBUG, LOG_NOTICE, LOG_NDELAY, LOG_CONS, LOG_PID, LOG_LOCAL0
from signal import signal, SIG_IGN, SIGINT, SIGHUP, SIGTERM, SIGQUIT

class Process(multiprocessing.Process):
    """
    Provides the minimal components of a multiprocessing.Process as
    used throughout PyCrawler.  These include a go event and syslog
    usage.
    """
    debug = False

    def __init__(self, go=None):
        """
        Keep self.name as its previously set value, or replace it with
        "Unnamed"

        If go is not provided, then create a self.go event, which
        self.prepare will set.
        """
        if not hasattr(self, "name"):
            self.name = "Unnamed"
        multiprocessing.Process.__init__(self, name=self.name)
        self.go = go or multiprocessing.Event()

    def prepare_process(self):
        """
        Set the go Event and open the syslog
        """
        #for sig in [SIGINT, SIGHUP, SIGTERM, SIGQUIT]:
        #    signal(sig, SIG_IGN)
        self.go.set()
        openlog(self.name, LOG_NDELAY|LOG_CONS|LOG_PID, LOG_LOCAL0)
        if not self.debug:
            setlogmask(LOG_UPTO(LOG_INFO))

    def stop(self):
        syslog(LOG_DEBUG, "Stop called.")
        self.go.clear()

def multi_syslog(level=LOG_DEBUG, msg=None):
    if msg is None:
        if type(level) is str:
            msg = level
            level = LOG_DEBUG
        else:
            raise Exception("")
    rows = msg.splitlines()
    def log(row):
        syslog(level, row)
    map(log, rows)
