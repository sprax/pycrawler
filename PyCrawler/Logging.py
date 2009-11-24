"""
Provides a reasonably pythonic logging toolset for multiprocessing,
including mechanisms for handling rapidly repeated log messages and
locking between different tools all writing to stderr.

This could possibly be replaced or improved by using python's new
logging module by integrating it with multiprocessing.
"""
#$Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import os
import sys
import Queue
import syslog
import multiprocessing
from time import sleep, time
from signal import signal, alarm, SIGALRM, SIGHUP, SIGINT, SIGQUIT, SIGABRT, SIGTERM, SIGPIPE, SIG_IGN

DELIMITER = "\t"

syslog_type = sys.stderr
def syslog_open(name):
    """Given a name, does an openlog. Given a file handle, saves it for later writing."""
    global syslog_type
    if hasattr(name, "write"):
        syslog_type = name
    else:
        syslog.openlog(name, syslog.LOG_NDELAY|syslog.LOG_CONS|syslog.LOG_PID, syslog.LOG_LOCAL0)
        syslog_type = None

# global variable for start time; used with log(..., relative=True)
start = 0
def log(msg, fh=syslog_type, timestamp=True, relative=True):
    """
    A low-level wrapper around the filehandle to which we are logging.
    Defaults to stderr.
    """
    if timestamp:
        t = time()
        if relative:
            global start
            t -= start
        msg = "%.2f%s%s" % (t, DELIMITER, msg)
    if syslog_type:
        syslog_type.write(msg)
        syslog_type.flush()
    else:
        syslog.syslog(syslog.LOG_ERR, msg)

def _construct_message(rec):
    """
    Takes an interable object and makes a string by converting each
    item to strings (with repr if necessary) and concatenated with
    tabs and a newline.
    """
    rec_str = []
    for field in rec:
        if isinstance(field, (int, float)):    field =  str(field)
        if not isinstance(field, basestring): field = repr(field)
        rec_str.append(field)
    return DELIMITER.join(rec_str) + "\n"

def maybelog(rec, level=1, verbosity=0):
    """
    If level is *below* the verbosity, then the rec gets passed to
    _construct_message and logged with log.  Note that this log
    method has rec first and it is not optional; contrast with
    ChangeLogger.log()
    """
    if level <= verbosity:
        log(_construct_message(rec))

class Logger(multiprocessing.Process):
    """
    A subclass of multiprocessing.Process that is the single point
    of entry to the logging filehandle.  This monitors repeat
    messages and logs dots accordingly.

    There are two external methods:

    Logger.stop()

    Logger.inQ.put(rec)

    We recommend not using put_nowait(), so that your process
    blocks if the Logger is backlogged.

    rec must be an iterable object, such as a tuple, and it is
    passed to _construct_message to make a string.
    """
    def __init__(self, verbosity):
        """
        Sets the verbosity level; sets up the inQ Queue and stop()
        method; initialize the state.
        """
        multiprocessing.Process.__init__(self, name="Logger")
        for sig in (SIGALRM, SIGHUP, SIGINT, SIGQUIT, SIGABRT, SIGTERM):
            signal(sig, SIG_IGN)
        self.verbosity = verbosity
        self.inQ = multiprocessing.Queue()
        self.go  = multiprocessing.Event()
        self.previous = ""
        global start
        start = time()

    def stop(self):
        "sleeps briefly to ensure inQ is empty before clearing self.go"
        sleep(0.3)
        self.go.clear()

    def run(self):
        """
        Repeatedly gets messages from inQ and logs them, unless
        they repeat the last message, in which case it logs a dot.
        """
        # initial dotting state
        self.dotting = False
        self.pending = True
        log(("Logger", self.pid,
              "entering logger loop; verbosity=%d" % self.verbosity),
             0, 3)
        sleep_interval = 0.5
        heartbeat_counter = 0
        heartbeat_interval = 10
        while self.pending:
            try:
                rec, level = self.inQ.get_nowait()
                heartbeat_counter = 0
            except Queue.Empty:
                sleep(sleep_interval)
                # only stop if Empty and not go
                self.pending = self.go.is_set()
                heartbeat_counter += sleep_interval
                if heartbeat_counter > heartbeat_interval:
                    rec = ("logger is alive after pause of %d seconds" % heartbeat_interval,)
                    level = 0
                else:
                    continue
            # we could use None as a kill signal, but let's try to
            # do it with explicit calls to Logger.stop()
            #if rec is None: break
            if level > self.verbosity: continue
            msg = _construct_message(rec)
            # if new message, log it, otherwise log dots.
            if msg != self.previous:
                self.previous = msg
                # if we were dotting, then we need to end the dots
                if self.dotting:
                    log("\n", timestamp=False)
                    self.dotting = False
                log(msg)
            else:
                self.dotting = True
                log(".", timestamp=False)
        # Since this process only consumes from this inQ, perhaps
        # there is something different we should do here?
        #self.inQ.close()
        #self.inQ.cancel_join_thread()
        log(("Logger", self.pid, "exiting",) , 0, 3)

class ChangeLogger:
    """
    A utility for keeping track of repeated log messages and the name
    of the class doing the logging.  To maintain consistency of dotted
    logging for repeated messages, if one wishes to run multiple
    instances of ChangeLogger or subclasses of ChangeLogger, then one
    should create a master ChangeLogger instances, and pass it into
    the constructor of all ChangeLoggers.  This master instance then
    manages locking and dotting.
    """
    def __init__(self, name="ChangeLogger", logger=None, verbosity=3):
        """
        Utility methods for logging.

        If present, master must be a ChangeLogger instance that this
        ChangeLogger uses to communicate with the output filehandle.
        verbosity is only used if master is None, and it sets the
        threshold for all log messages that get written to the
        filehandle.

        Any ChangeLogger instance or subclass can be used as a master.
        """
        self.name = name
        if logger is None:
            # start it immediately, because it must be a child of the
            # main process for the inQ to work (in current versions of
            # multiprocessing)
            logger = Logger(verbosity)
            logger.go.set()
            logger.start()
            self._am_master_logger = True
        else:
            self._am_master_logger = False
        self.logger = logger
        self._log_func = lambda step: step

    def stop(self):
        """If this instance created the logger, then stop it."""
        if self._am_master_logger:
            self.logger.stop()

    def set_msg_func(self, func):
        """
        Set a function to create log messages for this ChangeLogger
        instance.  func must accept an optional 'step' parameter and
        return a single output, which should generally be a
        basestring, but could be an integer or something else (which
        will get repr'ed into a string).  The purpose of the 'step'
        parameter is to allow your msg function to react differently
        at different steps in your program's opertions.
        """
        self._log_func = func

    def log(self, level=1, msg=None, step=None):
        """
        If msg is absent, then it calls the function provided to
        set_msg_func to get a msg.

        Creates a log record of (instance name, pid, step, msg, level)
        and logs it through the master Logger() instance.

        level is the first kwarg to this function, so, zum beispiel,
        you can simply call self.log(3) to generate a third level log
        message from the function passed into set_msg_func()
        """
        assert level <= 3, "We want to define only three levels of verbosity."
        if msg is None: 
            msg = self._log_func(step)
        if hasattr(self, "pid"):
            pid = self.pid
        else:
            pid = os.getpid()
        rec = (self.name, pid, msg)
        self.logger.inQ.put((rec, level))

class ChangeLoggerContainer(ChangeLogger, multiprocessing.Process):
    """
    A basic subclass of Process and ChangeLogger that has a self.go
    Event
    """
    def __init__(self, name="ChangeLoggerContainer", 
                  go=None, logger=None, verbosity=3):
        ChangeLogger.__init__(self, name, logger, verbosity)
        multiprocessing.Process.__init__(self, name=name)
        self.go = go or multiprocessing.Event()
        self.go.set()

    def run(self):
        """
        
        """
        # Ignore signals, so the parent has a chance to gracefully
        # request exit by calling stop()
        while self.go.is_set():
            sleep(1)

    def stop(self, a=None, b=None): 
        """
        Clears the go Event on ChangeLoggerContainer instances that have
        their own go event (rather than receiving it on instantiation).
        This function can be called by signal.signal
        """
        self.log(2, "stop(%s)" % a)
        if self.go is not None: self.go.clear()
        ChangeLogger.stop(self)

if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--syslog", dest="syslog", default=False, action="store_true", help="run tests with logging to syslog instead of stderr")
    parser.add_option("--delimiter", dest="delimiter", default="|", help="string to use for delimiting fields within a message")
    (options, args) = parser.parse_args()
    if options.syslog:
        syslog_open("pycrawler")
    DELIMITER = options.delimiter    
    class Test(ChangeLoggerContainer):
        def run(self):
            self.set_msg_func(
                lambda step: "count=%s, go=%s" % \
                    (self.count, self.go.is_set()))
            self.log(0, "starting")
            self.count = 0
            while self.go.is_set():
                self.log(0)
                sleep(.1)
                self.count += 1
                if self.count == 1:
                    for a in range(100):
                        self.log(0)
            self.log(0, "stopping")
            self.logger.stop()

    t1 = Test("test1")
    t2 = Test("test2", t1.go, t1.logger)

    def mlog(msg):
        maybelog(("__main__", msg), level=0, verbosity=3)

    def stop(a, b):
        mlog("received %s" % a)
        t1.stop()

    for sig in (SIGALRM, SIGHUP, SIGINT, SIGQUIT, SIGABRT, SIGTERM):
        signal(sig, stop)
    alarm(1)

    mlog("about to start")
    t1.start()
    t2.start()

    while len(multiprocessing.active_children()) > 0:
        sleep(1)
        mlog("waiting for children to stop before joining...")

    # If we don't do the while loop above, then this first join
    # (always?) throws an error about interupted system call.
    t1.join()
    mlog("t1.exitcode = %s" % t1.exitcode)
    mlog("joined 1, waiting for second...")

    t2.join()
    mlog("joined 2")
    mlog("t2.exitcode = %s" % t2.exitcode)

    print "If you got here without any tracebacks or having to kill it, then the test passed."
