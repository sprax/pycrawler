#!/usr/bin/python2.6
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
import pprint
import traceback
import multiprocessing
import multiprocessing.managers
from copy import copy
from time import time, sleep
from syslog import syslog, LOG_INFO, LOG_DEBUG, LOG_NOTICE
from Fetcher import Fetcher
from Process import Process, multi_syslog
from AnalyzerChain import FetchInfo
from CrawlStateManager import CrawlStateManager

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
    def __init__(self, go, inQ, id, hostbins, authkey=AUTHKEY):
        self.name = "Sender:%s" % id
        Process.__init__(self, go)
        self.inQ = inQ
        self.hostbins = hostbins
        self.authkey = authkey

    def run(self):
        """
        uses FetchClient to connect to each FetchServer's
        hostname:port specified as the values of hostbins, and
        sends the data that this FetchServer has accumulated for
        that other FetchServer
        """
        syslog(LOG_DEBUG, "Starting.")
        try:
            self.prepare_process()
            while self._go.is_set():
                try:
                    fetch_info = self.inQ.get_nowait()
                except Queue.Empty:
                    sleep(1)
                fmc = FetchServerClient(
                    self.hostbins.get_fetch_server(fetch_info.hostbin), 
                    self.authkey)
                fmc.put(fetch_info)
                fmc.close()
        except Exception, exc:
            multi_syslog(traceback.format_exc(exc))
        syslog(LOG_DEBUG, "Exiting.")

class FetchServer(Process):
    class ManagerClass(multiprocessing.managers.BaseManager): pass

    def __init__(self, go=None, address=("", PORT), authkey=AUTHKEY, debug=False):
        """
        Creates an inQ, reload Event, and relay Namespace.

        Registers go, reload, relay, and inQ with self.ManagerClass

        id is a hostname:port string that allows this FetchServer to
        find its hostbins in the config
        """
        self.id = "%s:%s" % address
        self.name = "FetchServer:%s" % self.id
        self._debug = debug
        Process.__init__(self)
        self.address = address
        self.authkey = authkey
        self.csm = None # CrawlStateManager created below
        self.manager = None # created below
        self.reload = multiprocessing.Event()
        self.reload.clear()
        mgr = multiprocessing.Manager()
        self.relay = mgr.Namespace()
        self.config = None
        self.inQ = multiprocessing.Queue(1000)
        self.outQ = None
        self.packerQ = None
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
        try:
            self.prepare_process()
            self.manager = self.ManagerClass(self.address, self.authkey)
            self.manager.start()
            # make a queue for passing records to sender
            self.outQ = multiprocessing.Queue(1000)
            self.packerQ = multiprocessing.Queue(1000)
            syslog(LOG_DEBUG, "Entering main loop")
            while self._go.is_set():
                if self.reload.is_set():
                    if self.valid_new_config():
                        self.config = copy(self.relay.config)
                        self.start_children()
                    self.reload.clear()
                if self.config is None:
                    syslog(LOG_DEBUG, "Waiting for config")
                    sleep(1)
                    continue
                # Get an AnalyzerChain. This allows csm to record data
                # as it streams out of fetcher.  The config could
                # cause csm to add more Analyzers to the chain.
                ac = self.csm.get_analyzerchain()
                while self._go.is_set() and not self.reload.is_set():
                    syslog(LOG_DEBUG, "Creating & start Fetcher")
                    # could do multiple fetchers here...
                    self.fetcher = Fetcher(
                        go = self._go,
                        outQ = ac.inQ,
                        packerQ = self.packerQ,
                        params = self.config["fetcher_options"])
                    self.fetcher.start()
                    while self._go.is_set() and self.fetcher.is_alive():
                        #syslog(LOG_DEBUG, "Fetcher is alive.")
                        #self.config["heart_beat"] = time()
                        sleep(1)
        except Exception, exc:
            multi_syslog(exc)
            self.stop()
            #for child in multiprocessing.active_children():
            #    try: child.terminate()
            #    except: pass
        finally:
            if self.manager:
                syslog("Attempting manager.shutdown()")
                self.manager.shutdown()
            while len(multiprocessing.active_children()) > 0:
                syslog("Waiting for: " + str(multiprocessing.active_children()))
                sleep(1)
            syslog("Exiting FetchServer.")

    def start_children(self):
        """
        Creates a CrawlStateManager and a Sender using self.config
        """
        # csm handles disk interactions with state
        syslog(LOG_DEBUG, "creating & starting CrawlStateManager")
        self.csm = CrawlStateManager(
            self._go, self.id, self.inQ, self.outQ, self.packerQ, self.config)
        self.csm.start()
        # launch self.Sender to send hostbins assigned to other
        # FetchServers
        self.sender = Sender(
            self._go,
            self.outQ,
            self.id,
            self.config["hostbins"],
            authkey = self.authkey)
        self.sender.start()

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
        elif not hasattr(config["hostbins"], "_fetch_servers"):
            syslog("invalid config: its hostbins is wrong type")
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
    """
    TODO: can this be a subclass of BaseManager?  Can this be cleaner?
    """
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
        yzable = FetchInfo(url=url)
        try:
            self.fm.put(yzable)
        except Exception, exc:
            multi_syslog(exc)

def default_port(hostnames=[""]):
    return ["%s:%s" % (hostname, PORT) for hostname in hostnames]

class Hostbins:
    def __init__(self, list_of_addresses):
        self._fetch_servers = list_of_addresses

    def __getinitargs__(self):
        "enable pickling"
        return (self._fetch_servers,)

    def get_fetch_server(self, hostbin):
        """
        Return the fetch server address for the given hostbin
        """
        if len(self._fetch_servers) == 1:
            return self._fetch_servers[0]
        # strip slashes
        hostbin = hostbin[:2] + hostbin[3:5] + hostbin[6:9]
        # convert string to int
        hostbin = int(hostbin, 16)
        # use modular arithmetic to index into _fetch_servers list
        hostbin = hostbin % len(self._fetch_servers)
        return self._fetch_servers[hostbin]
        
class TestHarness(Process):
    name = "FetchServerTestHarness"
    def __init__(self, debug=None):
        Process.__init__(self, go=None, debug=debug)
    def run(self):
        self.prepare_process()
        try:
            syslog("Creating & starting FetchServer")
            fs = FetchServer(debug=self._debug)
            fs.start()
            syslog("Creating & configurating FetchClient")
            fc = FetchClient()            
            fc.set_config({
                    "debug": self._debug,
                    "hostbins": Hostbins(default_port()),
                    "frac_to_fetch": 0.4,
                    "data_path": "/var/lib/pycrawler",
                    "fetcher_options": {
                        #"SIMULATE": 3,
                        "_debug": self._debug,
                        "DOWNLOAD_TIMEOUT":  60,
                        "FETCHER_TIMEOUT":   30000,
                        }
                    })
            syslog("FetchClient.add_url(\"http://cnn.com\")")
            fc.add_url("http://cnn.com")
            while self._go.is_set() and fs.is_alive():
                sleep(.1)
            # call stop on FetchClient, not FetchServer
            try:
                syslog("fc.stop()")
                fc.stop()
            except Exception, exc:
                syslog("FetchClient.stop() --> %s" % exc)
                try:
                    syslog("fs.stop()")
                    fs.stop()
                except Exception, exc:
                    syslog("FetchServer.stop() --> %s" % exc)
                    try:
                        fs.terminate()
                    except Exception, exc:
                        syslog("FetchServer.terminate() --> %s" % exc)
            # if anything is alive, terminating it
            for child in multiprocessing.active_children():
                syslog("Terminating: %s" % repr(child))
                try: 
                    child.terminate()
                except Exception, exc: 
                    multi_syslog(exc)
        except Exception, exc:
            multi_syslog(exc)
        finally:
            syslog("Exiting TestHarness.")

if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser("runs a TestHarness instance")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Sets debug to True everywhere.")
    (options, args) = parser.parse_args()

    test = TestHarness(debug=options.debug)
    test.start()

    from signal import signal, SIGINT, SIGHUP, SIGTERM, SIGQUIT
    def stop(a, b):
        print "Attempting test.stop()"
        test.stop()
    for sig in [SIGINT, SIGHUP, SIGTERM, SIGQUIT]:
        signal(sig, stop)

    print "Waiting for TestHarness to exit."
    while test.is_alive():
        sleep(1)
    
