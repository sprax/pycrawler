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

import os
import Queue
import daemon  # from PyPI
import pprint
import logging
import traceback
import multiprocessing
import multiprocessing.managers
from copy import copy
from time import time, sleep

from fetcher import Fetcher
from process import Process, multi_syslog
from crawl_state_manager import CrawlStateManager
from analyzer_chain import FetchInfo

PORT = 18041 # default port is the second prime number above 18000
AUTHKEY = "APPLE"

class FetchServer(Process):
    class ManagerClass(multiprocessing.managers.BaseManager):
        pass

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

        self.logger = logging.getLogger('PyCrawler.FetchServer.ManagerClass')

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
        self.ManagerClass.register("put", callable=self.inQ.put)
        self.ManagerClass.register("stop", callable=self.stop)
        self.ManagerClass.register("set_config", callable=self.set_config)

        self.ac = None
        self.fetcher = None

    def set_config(self, config):
        "Passes config into the relay"
        try:
            self.relay.config = config
            self.reload.set()
            self.logger.debug("set_config")
        except Exception, exc:
            multi_syslog(exc, logger=self.logger.warning)

    def run(self):
        """
        Starts instances of self.ManagerClass, and then waits for
        initialization, which provides:

            * list of FetchServer addresses and assigned hostbins

            * fetching parameters, including fraction of URLs to
              actually fetch (see below)

        Once initialized, it loops over these steps forever:

            0) handle reload event

            1) launch CrawlStateManager
        """
        try:
            self.prepare_process()
            self.manager = self.ManagerClass(self.address, self.authkey)
            self.manager.start()
            self.hostQ = multiprocessing.Queue(1000)
            self.logger.debug("Entering main loop")
            while not self._stop.is_set():
                if self.reload.is_set():
                    if self.valid_new_config():
                        self.config = copy(self.relay.config)
                        self.logger.debug("creating & starting CrawlStateManager")
                        if self.csm:
                            self.csm.stop()
                        self.csm = CrawlStateManager(
                            self._go, self.id, self.inQ, self.hostQ, self.config)
                        self.csm.start()
                    self.reload.clear()
                if self.config is None:
                    self.logger.debug("Waiting for config")
                    sleep(1)
                    continue
                # AnalyzerChain records data streaming out of fetchers
                if self.ac:
                    self.ac.stop()
                self.ac = self.csm.get_analyzerchain()
                while not self._stop.is_set() and not self.reload.is_set():
                    self.logger.debug("Creating & start Fetcher")
                    # could do multiple fetchers here...
                    self.fetcher = Fetcher(
                        go = self._go,
                        hostQ = self.hostQ,
                        outQ = self.ac.inQ,
                        params = self.config["fetcher_options"])
                    self.fetcher.start()
                    while not self._stop.is_set() and self.fetcher.is_alive():
                        #syslog(LOG_DEBUG, "Fetcher is alive.")
                        #self.config["heart_beat"] = time()
                        sleep(1)
            else:
                self.logger.debug("Stopping server.")
        except Exception, exc:
            multi_syslog(exc, logger=self.logger.warning)
            self.stop()
            #for child in multiprocessing.active_children():
            #    try: child.terminate()
            #    except: pass
        finally:
            if self.manager and hasattr(self.manager, 'shutdown'):
                self.logger.info("Attempting manager.shutdown()")
                self.manager.shutdown()
            if self.fetcher:
                self.fetcher.stop()
            if self.csm:
                self.csm.stop()
            if self.ac:
                self.ac.stop()
            while len(multiprocessing.active_children()) > 0:
                self.logger.info("Waiting for: " + str(multiprocessing.active_children()))
                sleep(1)
            self.logger.info("Exiting FetchServer.")
        try:
            self.cleanup_process()
        except Exception, exc:
            multi_syslog(exc, logger=self.logger.warning)

    def valid_new_config(self):
        """returns bool indicating whether 'config' is valid, i.e. has
        1) hostbins containing this FetchServer's id
        2) frac_to_fetch that is a float 
        3) fetcher_options that is a dict
        """
        if not hasattr(self.relay, "config"):
            self.logger.info("invalid config: no config set on relay")
            return False
        else:
            config = self.relay.config
        if "hostbins" not in config:
            self.logger.info("invalid config: lacks hostbins")
            return False
        elif not hasattr(config["hostbins"], "_fetch_servers"):
            self.logger.info("invalid config: its hostbins is wrong type")
            return False
        elif "frac_to_fetch" not in config or \
                not isinstance(config["frac_to_fetch"], float):
            self.logger.info("invalid config: frac_to_fetch should be float")
            return False
        elif "fetcher_options" not in config or \
                not isinstance(config["fetcher_options"], dict):
            self.logger.info("invalid config: missing fetcher_options")
            return False
        # must be valid
        return True

class FetchClient:
    """
    TODO: can this be a subclass of BaseManager?  Can this be cleaner?
    """
    def __init__(self, address=("", PORT), authkey=AUTHKEY):
        class LocalFetchManager(multiprocessing.managers.BaseManager):
            pass
        LocalFetchManager.register("put")
        LocalFetchManager.register("stop")
        LocalFetchManager.register("set_config")
        self.fm = LocalFetchManager(address, authkey)
        self.fm.connect()
        self.stop = self.fm.stop
        self.set_config = self.fm.set_config

        self.logger = logging.getLogger('PyCrawler.Server.FetchClient')

    def add_url(self, url):
        """
        Create a new URL record and add it to this FetchServer
        """
        yzable = FetchInfo.create(url=url)
        try:
            self.fm.put(yzable)
        except Exception, exc:
            multi_syslog(exc, logger=self.logger.warning)

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
        self.logger = logging.getLogger('PyCrawler.TestHarness')

    def run(self):
        self.prepare_process()
        try:
            self.logger.info("Creating & starting FetchServer")
            fs = FetchServer(debug=self._debug)
            fs.start()
            self.logger.info("Creating & configurating FetchClient")
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
            self.logger.info("FetchClient.add_url(\"http://cnn.com\")")
            fc.add_url("http://cnn.com")
            while not self._stop.is_set() and fs.is_alive():
                sleep(.1)
            # call stop on FetchClient, not FetchServer
            try:
                self.logger.info("fc.stop()")
                fc.stop()
            except Exception, exc:
                self.logger.info("FetchClient.stop() --> %s" % exc)
                try:
                    self.logger.info("fs.stop()")
                    fs.stop()
                except Exception, exc:
                    self.logger.info("FetchServer.stop() --> %s" % exc)
                    try:
                        fs.terminate()
                    except Exception, exc:
                        self.logger.info("FetchServer.terminate() --> %s" % exc)
            # if anything is alive, terminating it
            for child in multiprocessing.active_children():
                self.logger.info("Terminating: %s" % repr(child))
                try: 
                    child.terminate()
                except Exception, exc: 
                    multi_syslog(exc, logger=self.logger.warning)
        except Exception, exc:
            multi_syslog(exc, logger=self.logger.warning)
        finally:
            self.logger.info("Exiting TestHarness.")

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
    
