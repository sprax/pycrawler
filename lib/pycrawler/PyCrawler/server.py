#!/usr/bin/python2.6
"""
Provides a multiprocessing.managers.BaseManager for interacting with a
CrawlStateManager.

This runs Fetcher instances on batches of URLs provided by the
CrawlStateManager.
"""

__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank.  Copyright 2010, Nokia Corporation."
__license__ = "MIT License"
__version__ = "0.1"
__revision__ = "$Id$"

import os
import logging
import multiprocessing
import multiprocessingng
import multiprocessingng.managers
from copy import copy
from time import time, sleep

from fetcher import Fetcher
from process import Process, multi_syslog
from crawl_state_manager import CrawlStateManager
from analyzer_chain import FetchInfo, FetchInfoFIFO
from new_link_queue import URLChecker, HostSpreader, get_new_link_queue_analyzerchain

PORT = 18041 # default port is the second prime number above 18000
AUTHKEY = "APPLE"

class FetchServer(Process):
    class ManagerClass(multiprocessing.managers.BaseManager):
        pass

    def __init__(self, go=None, address=("", PORT), authkey=AUTHKEY, qdir=None, debug=False):
        """
        Creates an inQ, reload Event, and relay Namespace.

        Registers go, reload, relay, and inQ with self.ManagerClass

        """
        self.id = "%s:%s" % address
        self.name = "FetchServer:%s" % self.id
        self._debug = debug

        assert isinstance(qdir, str)

        Process.__init__(self)

        self.address = address
        self.authkey = authkey
        self.csm = None # CrawlStateManager created below
        self.manager = None # created below
        self.reload = multiprocessing.Event()
        self.reload.clear()
        self.relay = None
        self.config = None

        self.ManagerClass.register("put", callable=self.put)
        self.ManagerClass.register("stop", callable=self.stop)
        self.ManagerClass.register("set_config", callable=self.set_config)
        self.ManagerClass.register("get_config", callable=self.get_config)
        self.ManagerClass.register("reload", callable=self.reload.set)

        self.qdir = qdir

        self.ac = None
        self.link_ac = None
        self.fetcher = None

    def get_config(self):
        if hasattr(self.relay, 'config'):
            return self.relay.config
        else:
            return None

    def put(self, yzable):
        assert self.link_ac
        return self.link_ac.inQ.put(yzable)

    def set_config(self, config):
        "Passes config into the relay"
        #self.logger.debug('set_config: %r' % config)
        try:
            self.relay.config = config
            self.reload.set()
            self.logger.debug("set_config")
        except Exception, exc:
            multi_syslog(exc, logger=self.logger.warning)

    def prepare_process(self):
        super(FetchServer, self).prepare_process()
        self.logger = logging.getLogger('PyCrawler.FetchServer')
        self.link_ac = get_new_link_queue_analyzerchain(qdir = self.qdir,
                                                        whitelist = 'metacarta.txt')
        self.logger.info("Loaded link_ac")

    def run_once(self, qgen):
        """ Do one crawl cycle. """
        if self.reload.is_set():
            self.logger.info("Reloaded!")
            if self.valid_new_config():
                self.config = copy(self.relay.config)
                self.logger.debug("creating & starting CrawlStateManager")
                self.reload.clear()

        if self.config is None:
            self.logger.debug("Waiting for config")
            for i in range(60):
                if self.reload.wait(10) or self._stop.is_set():
                    break
            return

        try:
            inQ = qgen.next()
        except:
            return

        self.csm = CrawlStateManager(
            self._go, self.id, inQ, self.fetchQ, self.config)
        self.csm.start()
        self.ac = self.csm.get_analyzerchain(self.link_ac.inQ)

        self.logger.debug("Creating & start Fetcher")
        # could do multiple fetchers here...
        self.fetcher = Fetcher(
            go = self._go,
            inQ = self.fetchQ,
            outQ = self.ac.inQ,
            params = self.config["fetcher_options"])
        self.fetcher.start()
        while self.fetcher.is_alive():
            if self._stop.is_set() or self._stop.wait(1):
                self.fetcher.stop()
                break
        self.ac.stop()
        self.csm.stop()

    def queues(self):
        for i in os.listdir(self.qdir):
            path = os.path.join(self.qdir, i)
            if os.path.isdir(path):
                yield FetchInfoFIFO(path)

    def run(self):
        """
        Starts instances of self.ManagerClass, and then waits for
        initialization, which provides:

            * fetching parameters, including fraction of URLs to
              actually fetch (see below)

        Once initialized, it loops over these steps forever:

            0) handle reload event

            1) launch CrawlStateManager
        """
        try:
            self.prepare_process()
            self.manager = self.ManagerClass(self.address, self.authkey)
            self.relay = self.manager.Namespace()
            self.manager.start()

            # We need to wait here, so we can properly shut it down later.
            # Unfortunately, there is a delay between starting a manager, and
            # when you can call shutdown()
            while not hasattr(self.manager, 'shutdown'):
                logger.debug("Waiting until manager fully started!")
                sleep(0.1)

            self.fetchQ = multiprocessing.Queue(1000)
            self.logger.debug("Entering main loop")
            qgen = self.queues()
            while True:
                if self._stop.is_set():
                    break
                self.run_once(qgen)
            else:
                self.logger.debug("Stopping server.")
        except Exception, exc:
            multi_syslog(exc, logger=self.logger.warning)
            self.stop()
            #for child in multiprocessing.active_children():
            #    try: child.terminate()
            #    except: pass
        finally:
            if self.link_ac:
                self.logger.info("Stopping link_ac")
                self.link_ac.stop()
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
        2) frac_to_fetch that is a float 
        3) fetcher_options that is a dict
        """
        if not hasattr(self.relay, "config"):
            self.logger.info("invalid config: no config set on relay")
            return False
        else:
            config = self.relay.config
        if "frac_to_fetch" not in config or \
                not isinstance(config["frac_to_fetch"], float):
            self.logger.info("invalid config: frac_to_fetch should be float")
            return False
        elif "fetcher_options" not in config or \
                not isinstance(config["fetcher_options"], dict):
            self.logger.info("invalid config: missing fetcher_options")
            return False
        # must be valid
        return True

class FetchClient(object):
    """
    TODO: can this be a subclass of BaseManager?  Can this be cleaner?
    """
    def __init__(self, address=("", PORT), authkey=AUTHKEY):
        class LocalFetchManager(multiprocessingng.managers.BaseManager):
            pass
        LocalFetchManager.register("put")
        LocalFetchManager.register("stop")
        LocalFetchManager.register("set_config")
        LocalFetchManager.register("get_config")
        LocalFetchManager.register("reload")
        
        self.fm = LocalFetchManager(address, authkey,
                                    timeout=1
                                    )
        self.fm.connect(timeout=1)
        self.stop = self.fm.stop
        self.set_config = self.fm.set_config
        self.get_config = self.fm.get_config
        self.reload = self.fm.reload

        self.logger = logging.getLogger('PyCrawler.Server.FetchClient')

    def add_url(self, url):
        """
        Create a new URL record and add it to this FetchServer
        """
        yzable = FetchInfo.create(url=url)
        print 'Adding %r' % yzable
        try:
            self.fm.put(yzable)
        except Exception, exc:
            multi_syslog(exc, logger=self.logger.warning)

def default_port(hostnames=[""]):
    return ["%s:%s" % (hostname, PORT) for hostname in hostnames]

class TestHarness(Process):
    name = "FetchServerTestHarness"
    def __init__(self, debug=None):
        Process.__init__(self, go=None, debug=debug)
        self.logger = logging.getLogger('PyCrawler.TestHarness')

    def run(self):
        self.prepare_process()
        try:
            self.logger.info("Creating & starting FetchServer")
            fs = FetchServer(debug=self._debug, qdir='TestHarnessQueue')
            fs.start()

            sleep(1)

            self.logger.info("Creating & configurating FetchClient")

            fc = FetchClient()
            fc.set_config({
                    "debug": self._debug,
                    "frac_to_fetch": 0.4,
                    "data_path": "/var/lib/pycrawler",
                    "fetcher_options": {
                        #"SIMULATE": 3,
                        "_debug": self._debug,
                        "DOWNLOAD_TIMEOUT":  60,
                        "FETCHER_TIMEOUT":   30000,
                        }
                    })

            self.logger.info("configured!")

            #self.logger.info("FetchClient.add_url(\"http://cnn.com\")")
            #fc.add_url("http://cnn.com")
            while not self._stop.is_set() and fs.is_alive():
                self._stop.wait(1)

            # call stop on FetchClient, not FetchServer
            try:
                self.logger.info("fc.stop()")
                fc.stop()
                if hasattr(fc.fm, 'shutdown'):
                    fc.fm.shutdown()
            except Exception, exc:
                self.logger.info("FetchClient.stop() --> %s" % exc)


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
    
