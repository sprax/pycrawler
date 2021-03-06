
import os
import errno

from PyCrawler import AnalyzerChain, HostSpreader, FetchInfo, FetchInfoFIFO
from PyCrawler.analyzer_chain import Analyzable
from PyCrawler.new_link_queue import get_new_link_queue_analyzerchain
from PersistentQueue import SetDB, RecordFIFO

from time import sleep, time
from signal import signal, alarm, SIGALRM, SIGHUP, SIGINT, SIGQUIT, SIGABRT, SIGTERM, SIGPIPE, SIG_IGN

import multiprocessing
import logging
import shutil
import tempfile

class TestHostSpreader(object):
    
    def setUp(self):
        self.dbname = tempfile.NamedTemporaryFile(prefix=__name__ + '-urlchecker.', suffix='.db')
        self.whitelist = tempfile.NamedTemporaryFile(prefix=__name__ + '-whitelist.', suffix='.txt')

        print >>self.whitelist, "www.example.com"
        self.whitelist.flush()

        self.qdir = tempfile.mkdtemp(prefix=__name__ + '.', suffix='.queue')
        self.dbname.delete = False


    def tearDown(self):
        self.dbname.delete = True
        self.whitelist.delete = True

        try:
            shutil.rmtree(self.qdir)
        except OSError as exc:
            if exc.errno != errno.ENOENT:
                raise

        self.whitelist.close()
        self.dbname.close()

    def check_seen(self, urls, reverse=False):
        """ Ensure that all urls in urls are in the urlseen database. """
        urlseen = SetDB(self.dbname.name)
        if reverse:
            for url in urls:
                assert url not in urlseen
        else:
            for url in urls:
                assert url in urlseen

    def check_queues(self, urls):
        """ make sure queues have correct data. """
        url_set = frozenset(urls)
        unseen_url_set = set(urls)

        queues = [FetchInfoFIFO(os.path.join(self.qdir, d)) for d in os.listdir(self.qdir)
                  if os.path.isdir(os.path.join(self.qdir, d))]

        for q in queues:
            for info in q:
                url = info.hostkey + info.relurl
                assert url in url_set
                assert url in unseen_url_set
                unseen_url_set.remove(url)

    def test_host_spreader(self, with_broken_analyzer=False, timeout=5):
        """ Ensure host spreader works correctly. """

        ac = None

        def stop(a=None, b=None):
            logging.debug("received %s" % a)
            ac.stop()

        for sig in (SIGALRM, SIGHUP, SIGINT, SIGQUIT, SIGABRT, SIGTERM):
            signal(sig, stop)

        ac = get_new_link_queue_analyzerchain(qdir = self.qdir, debug=True,
                                              dbname = self.dbname.name,
                                              whitelist = self.whitelist.name)

        text = "This is a test document." #urllib.urlopen(hostkey).read()

        goodurls = []
        badurls = []

        try:

            # Make sure that this doesn't crash the pipeline.
            ac.inQ.put(Analyzable())

            for num in range(10):
                goodurls.append('http://www.example.com/%d' % num)

            # These should *not* make it past.
            for num in range(10):
                badurls.append('http://ftp.example.com/%d' % num)
                    
            for url in (goodurls + badurls) * 2:
                u = FetchInfo.create(**{
                        "url": url,
                        "depth":   0, 
                        "last_modified": 0,
                        "raw_data": text
                        }) 
                ac.inQ.put(u)

            ac.stop()

            for i in range(timeout):
                actives = multiprocessing.active_children()
                logging.info("waiting for children: %s" % actives)
                if len(actives) == 0:
                    break
                logging.info("in-flight: %d queue-size: %d" % \
                                 (ac.in_flight.value, ac.inQ.qsize()))
                sleep(1)
            else:
                raise Exception("Failed after %d seconds" % timeout)
        finally:
            ac.stop()
            for i in range(20):
                if multiprocessing.active_children():
                    sleep(0.5)

        self.check_seen(badurls, reverse=True)
        self.check_seen(goodurls)
        self.check_queues(goodurls)

