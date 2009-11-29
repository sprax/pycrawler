"""
Provides a multiprocessing.managers.BaseManager for interacting with a
CrawlStateManager.

This runs Fetcher instances on batches of URLs provided by the
CrawlStateManager.
"""
#$Id:$
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
from PersistentQueue import PersistentQueue
import multiprocessing
import multiprocessing.managers
from copy import copy
from time import time, sleep
from syslog import syslog, openlog, LOG_INFO, LOG_DEBUG, LOG_NOTICE, LOG_NDELAY, LOG_CONS, LOG_PID, LOG_LOCAL0
from Fetcher import Fetcher
from CrawlStateManager import CrawlStateManager

PORT = 18041 # default port is the second prime number above 18000
AUTHKEY = "APPLE"
DATA_PATH = "/var/lib/pycrawler"

class Sender(multiprocessing.Process):
    def __init__(self, go, id, hostbins, authkey):
        multiprocessing.Process.__init__(self, name="Sender:%s" % id)
        self.go = go
        self.id = id
        self.hostbins = hostbins
        self.authkey = authkey

    def log(self, priority=LOG_DEBUG, msg=None, step=None):
        if msg is None: msg = self.msg(step)
        syslog(priority, msg)

    def msg(self, step=None):
        return step and "%s: " % step or ""

    def run(self):
        """
        uses FetchClient to connect to each FetchServer's
        hostname:port specified as the values of hostbins, and
        sends the data that this FetchServer has accumulated for
        that other FetchServer
        """
        openlog(self.name, LOG_NDELAY|LOG_CONS|LOG_PID, LOG_LOCAL0)
        self.log(msg="Starting")
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
                    self.log(msg="Blocking on %s" % bin)
                    sleep(1)
            self.log(msg="Done with %s" % bin)
        self.log(msg="All done.")

class FetchServer(multiprocessing.Process):
    class ManagerClass(multiprocessing.managers.BaseManager): pass

    def __init__(self, go=None, address=("", PORT), authkey=AUTHKEY, 
                 data_path=DATA_PATH):
        """
        Creates an inQ, reload Event, and relay Namespace.

        Registers go, reload, relay, and inQ with self.ManagerClass

        id is a hostname:port string that allows this FetchServer to
        find its hostbins in the config
        """
        self.id = "%s:%s" % address
        multiprocessing.Process.__init__(self, name="FetchServer:%s" % self.id)
        self.go = go or multiprocessing.Event()
        self.go.set()
        self.data_path = data_path
        self.inQ = PersistentQueue(data_path=os.path.join(self.data_path, "inQ"))
        self.reload = multiprocessing.Event()
        self.reload.clear()
        mgr = multiprocessing.Manager()
        self.relay = mgr.Namespace()
        self.config = None
        self.ManagerClass.register("put_urlrecs", callable=self.inQ.put)
        self.ManagerClass.register("stop", callable=self.go.clear)
        self.ManagerClass.register("reload", callable=self.reload.set)
        self.ManagerClass.register("set_config", callable=self.set_config)
        self.address = address
        self.authkey = authkey
        self.csm = None # CrawlStateManager created below
        self.manager = None # created below

    def set_config(self, config):
        """
        Passes config into the relay
        """
        self.relay.config = config

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
        self.manager = self.ManagerClass(self.address, self.authkey)
        self.manager.start()
        self.log(step="entering main loop")
        while self.go.is_set():
            if self.reload.is_set():
                self.log(step="reload is set")
                if hasattr(self.relay, "config") and \
                        self.valid_config(self.relay.config):
                    self.log(step="got valid config")
                    self.config = copy(self.relay.config)
                    self.hostbins = self.config["hostbins"][self.id]
                    # csm handles disk interactions with state
                    self.log(step="creating & starting CrawlStateManager")
                    self.csm = CrawlStateManager(self.config)
                    self.csm.start()
                else:
                    self.log(msg="got invalid config")
                    self.config = None
                    self.csm = None
                self.reload.clear()
            if self.config is None:
                sleep(1)
                self.log(msg="waiting for config")
                continue
            # get all data from inQ, and import into crawl state
            self.log(step="importing received files into csm")
            self.inQ.transfer_to(self.csm.inQ)
            self.csm.prepare()
            # Get an AnalyzerChain. This allows csm to record data as
            # it streams out of fetcher.  The config could cause csm
            # to add more Analyzers to the chain.
            self.log(step="getting an AnalyzerChain")
            ac = self.csm.get_analyzerchain()
            # Create a fetcher
            self.log(step="making a Fetcher")
            self.fetcher = Fetcher(
                outQ = ac.inQ,
                params = self.config["fetcher_options"],
                )
            # Get a URLs from csm, these will be selected by scoring
            self.log(step="getting packer from csm; passing into fetcher")
            self.fetcher.packer.expand(self.csm.get_packer_dump())
            self.log(step="starting fetcher")
            self.fetcher.start()
            # wait for fetcher to finish
            while self.fetcher.is_alive():
                self.log(msg="fetcher is alive, setting heart_beat")
                self.config["heart_beat"] = time()
                sleep(1)
            ac.stop()
        if self.csm is not None:
            self.csm.stop()
        self.log(msg="exiting main loop")

    def valid_config(self, config):
        """returns bool indicating whether 'config' is valid, i.e. has
        1) hostbins containing this FetchServer's id
        2) frac_to_fetch that is a float 
        3) fetcher_options that is a dict
        """
        if "hostbins" not in config:
            self.log(msg="invalid config: lacks hostbins")
            return False
        elif self.id not in config["hostbins"]:
            self.log(msg="invalid config: its hostbins lacks %s" % self.id)
            return False
        elif "frac_to_fetch" not in config or \
                not isinstance(config["frac_to_fetch"], float):
            self.log(msg="invalid config: frac_to_fetch should be float")
            return False
        elif "fetcher_options" not in config or \
                not isinstance(config["fetcher_options"], dict):
            self.log(msg="invalid config: missing fetcher_options")
            return False
        # must be valid
        return True

class FetchClient:
    def __init__(self, address=("", PORT), authkey=AUTHKEY):
        class LocalFetchManager(multiprocessing.managers.BaseManager): pass
        LocalFetchManager.register("get_inQ")
        LocalFetchManager.register("stop")
        LocalFetchManager.register("reload")
        LocalFetchManager.register("set_config")
        self.fm = LocalFetchManager(address, authkey)
        self.fm.connect()
        self.stop = self.fm.stop
    def set_config(self, config):
        self.fm.set_config(config)
        self.fm.reload()

def default_id(hostname):
    return "%s:%s" % (hostname, PORT)

def get_ranges(num):
    assert num > 0 and isinstance(num, int)
    step_size = float.fromhex("F" * num)
    ranges = []
    for i in range(num):
        ranges.append([step_size * num, step_size * (num + 1)])
    return ranges

def test():
    fs = FetchServer()
    fs.start()
    fc = FetchClient()
    fc.set_config({
            "hostbins": {default_id(""): get_ranges(1)[0]},
            "frac_to_fetch": 0.4,
            "fetcher_options": {
                "SIMULATE": 3,
                "DOWNLOAD_TIMEOUT":  60,
                "FETCHER_TIMEOUT":   30000,
                }
            })
    sleep(3)
    fc.stop()
    while fs.is_alive():
        sleep(1)
        syslog(LOG_DEBUG, "waiting for FetchServer to stop")
    cl.stop()

if __name__ == "__main__":
    test()
    
