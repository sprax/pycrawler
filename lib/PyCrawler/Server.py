"""
Provides a multiprocessing.managers.BaseManager for interacting with a
CrawlStateManager.

This runs Fetcher instances on batches of URLs provided by the
CrawlStateManager.
"""
#$Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
"""
TODO

Actually finish this --- this does not work yet; not done.

"""
import os
import Queue
import daemon  # from PyPI
import traceback
import multiprocessing
import multiprocessing.managers
from copy import copy
from time import time, sleep
from syslog import syslog, LOG_INFO, LOG_DEBUG, LOG_NOTICE
from Fetcher import Fetcher
from Process import Process
from CrawlStateManager import CrawlStateManager, make_recs_from_url

# using older than python2.6...
import ctypes
def floatfromhex(my_hex):
    my_int = int(my_hex, 16)                              # convert from hex to a Python int
    cp = ctypes.pointer(ctypes.c_int(my_int))             # make this into a c integer
    fp = ctypes.cast(cp, ctypes.POINTER(ctypes.c_float))  # cast the int pointer to a float pointer
    return fp.contents.value         # dereference the pointer, get the float

PORT = 18041 # default port is the second prime number above 18000
AUTHKEY = "APPLE"

class Sender(Process):
    def __init__(self, go, id, hostbins, authkey):
        self.id = id
        self.name = "Sender:%s" % id
        Process.__init__(self, go)
        self.hostbins = hostbins
        self.authkey = authkey

    def run(self):
        """
        uses FetchClient to connect to each FetchServer's
        hostname:port specified as the values of hostbins, and
        sends the data that this FetchServer has accumulated for
        that other FetchServer
        """
        self.prepare_process()
        syslog("Starting")
        sys.exit()
        for bin in self.hostbins:
            # skip if this FetchServer owns this bin
            if self.hostbins[bin] == self.id:
                continue
            fmc = FetchServerClient(self.hostbins[bin], self.authkey)
            for rec in DB.get_records(bin):
                try:
                    fmc.inQ.put_nowait(rec)
                except Queue.Full:
                    syslog("Blocking on %s" % bin)
                    sleep(1)
            syslog("Done with %s" % bin)
        syslog("All done.")

class FetchServer(Process):
    class ManagerClass(multiprocessing.managers.BaseManager): pass

    def __init__(self, go=None, address=("", PORT), authkey=AUTHKEY):
        """
        Creates an inQ, reload Event, and relay Namespace.

        Registers go, reload, relay, and inQ with self.ManagerClass

        id is a hostname:port string that allows this FetchServer to
        find its hostbins in the config
        """
        self.id = "%s:%s" % address
        self.name = "FetchServer:%s" % self.id
        Process.__init__(self)
        self.address = address
        self.authkey = authkey
        self.csm = None # CrawlStateManager created below
        self.manager = None # created below
        self.reload = multiprocessing.Event()
        self.reload.clear()
        mgr = multiprocessing.Manager()
        self.relay = mgr.Namespace()
        self.relay.next_packer = None
        self.config = None
        self.inQ = multiprocessing.Queue(1000)
        self.ManagerClass.register("put", callable=self.inQ.put)
        self.ManagerClass.register("stop", callable=self.stop)
        self.ManagerClass.register("set_config", callable=self.set_config)

    def set_config(self, config):
        """
        Passes config into the relay
        """
        try:
            self.relay.config = config
            self.reload.set()
        except Exception, exc:
            syslog(traceback.format_exc(exc))
        syslog("finished calling reload.set()")

    def run(self):
        """
        Starts instances of self.ManagerClass, and then waits for
        initialization, which provides:

            * list of FetchServer addresses and assigned hostbins

            * fetching parameters, including fraction of URLs to
              actually fetch (see below)

        Once initialized, it loops over these steps forever:

            0) handle reload event

            1) get files from receiver, hand them to CrawlStateManager

            2) launch Fetcher on highest scoring fraction of URLs in
            hostbins assigned to this FetchServer

            3) launch Sender to send hostbins assigned to other
            FetchServers

            4) wait for fetchers and sender to finish

        """
        self.prepare_process()
        self.manager = self.ManagerClass(self.address, self.authkey)
        self.manager.start()
        syslog(LOG_DEBUG, "Entering main loop")
        try:
            while self.go.is_set():
                if self.reload.is_set():
                    if self.valid_new_config():
                        syslog(LOG_DEBUG, "got valid config")
                        self.config = copy(self.relay.config)
                        # csm handles disk interactions with state
                        syslog(LOG_DEBUG, "creating & starting CrawlStateManager")
                        self.csm = CrawlStateManager(
                            self.inQ, self.relay, self.config)
                        self.csm.start()
                    self.reload.clear()
                if self.config is None:
                    syslog(LOG_DEBUG, "waiting for config")
                    sleep(1)
                    continue
                self.csm.prepare_state()
                # Get a URLs from csm, these will be selected by scoring
                if self.relay.next_packer is None:
                    syslog(LOG_DEBUG, "Waiting for packer...")
                    sleep(1)
                    continue
                else:
                    packer = copy(self.relay.next_packer)
                    self.relay.next_packer = None
                    syslog(LOG_DEBUG, "Got a packer with %d hosts" % len(packer.hosts))
                # Get an AnalyzerChain. This allows csm to record data as
                # it streams out of fetcher.  The config could cause csm
                # to add more Analyzers to the chain.
                syslog(LOG_DEBUG, "Getting an AnalyzerChain")
                ac = self.csm.get_analyzerchain()
                syslog(LOG_DEBUG, "Creating a Fetcher")
                self.fetcher = Fetcher(
                    outQ = ac.inQ,
                    params = self.config["fetcher_options"],
                    )
                # replace fetcher's packer before starting it
                self.fetcher.packer = packer
                syslog("starting fetcher")
                self.fetcher.start()
                while self.fetcher.is_alive():
                    syslog(LOG_DEBUG, "Fetcher is alive")
                    self.config["heart_beat"] = time()
                    sleep(1)
                ac.stop()
        except Exception, exc:
            syslog(traceback.format_exc(exc))
        syslog("stopping csm")
        if self.csm is not None:
            self.csm.stop()
        syslog("calling manager.shutdown()")
        self.manager.shutdown()
        while len(multiprocessing.active_children()) > 0:
            syslog("waiting for: " + str(multiprocessing.active_children()))
            sleep(1)
        syslog("exiting main loop")

    def valid_new_config(self):
        """returns bool indicating whether 'config' is valid, i.e. has
        1) hostbins containing this FetchServer's id
        2) frac_to_fetch that is a float 
        3) fetcher_options that is a dict
        """
        if not hasattr(self.relay, "config"):
            syslog("invalid config: no config set on relay")
            return False
        else:
            config = self.relay.config
        if "hostbins" not in config:
            syslog("invalid config: lacks hostbins")
            return False
        elif self.id not in config["hostbins"]:
            syslog("invalid config: its hostbins lacks %s" % self.id)
            return False
        elif "frac_to_fetch" not in config or \
                not isinstance(config["frac_to_fetch"], float):
            syslog("invalid config: frac_to_fetch should be float")
            return False
        elif "fetcher_options" not in config or \
                not isinstance(config["fetcher_options"], dict):
            syslog("invalid config: missing fetcher_options")
            return False
        # must be valid
        return True

class FetchClient:
    def __init__(self, address=("", PORT), authkey=AUTHKEY):
        class LocalFetchManager(multiprocessing.managers.BaseManager): pass
        LocalFetchManager.register("put")
        LocalFetchManager.register("stop")
        LocalFetchManager.register("set_config")
        self.fm = LocalFetchManager(address, authkey)
        self.fm.connect()
        self.stop = self.fm.stop
        self.set_config = self.fm.set_config

    def add_url(self, url):
        """
        Create a new URL record and add it to this FetchServer
        """
        recs = make_recs_from_url(url)
        for rec in recs:
            try:
                self.fm.put(rec)
            except Exception, exc:
                syslog(LOG_NOTICE, traceback.format_exc(exc))

def default_id(hostname):
    return "%s:%s" % (hostname, PORT)

def get_ranges(num):
    assert num > 0 and isinstance(num, int)
    step_size = floatfromhex("F" * num)
    ranges = []
    for i in range(num):
        ranges.append([step_size * num, step_size * (num + 1)])
    return ranges

class TestHarness(Process):
    name = "FetchServerTestHarness"
    debug = True
    def run(self):
        self.prepare_process()
        syslog("making a FetchServer")
        fs = FetchServer()
        fs.start()
        fc = FetchClient()
        syslog("calling FetchClient.set_config")
        fc.set_config({
                "debug": False,
                "hostbins": {default_id(""): get_ranges(1)[0]},
                "frac_to_fetch": 0.4,
                "data_path": "/var/lib/pycrawler",
                "fetcher_options": {
                    #"SIMULATE": 3,
                    "DOWNLOAD_TIMEOUT":  60,
                    "FETCHER_TIMEOUT":   30000,
                    }
                })
        syslog("done with FetchClient.set_config")
        sleep(3)
        syslog("adding url")
        fc.add_url("http://cnn.com")
        syslog("add_url returned")
        #sleep(30)
        #fc.stop()
        #syslog("called stop on FetchClient")
        while fs.is_alive():
            sleep(1)
            syslog("waiting for: " + str(multiprocessing.active_children()))
        syslog("Test is done.")

if __name__ == "__main__":
    test = TestHarness()
    test.start()

    from signal import signal, SIGINT, SIGHUP, SIGTERM, SIGQUIT
    def stop(a, b):
        print "attempting to stop test"
        test.stop()
    for sig in [SIGINT, SIGHUP, SIGTERM, SIGQUIT]:
        signal(sig, stop)

    print "waiting"
    while test.is_alive():
        sleep(1)
    
