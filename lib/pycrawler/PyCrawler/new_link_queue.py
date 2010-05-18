#!/usr/bin/env python2.6

"""
Deal with queue for new links.
"""

__copyright__ = "Copyright 2010, Nokia Corporation"
__license__ = "MIT License"
__version__ = "0.1"
__revision__ = "$Id$"

import os
import errno

from analyzer_chain import Analyzer, AnalyzerChain, FetchInfo, FetchInfoFIFO
from PersistentQueue import SetDB

class URLChecker(Analyzer):
    """ Checks if we know about the URL.

    Does this need to be a process?

    Like Analyzers, we should perhaps split notion of process vs.
    coroutine, and have a multiprocessing coroutine wrapper, and
    a simple in-process coroutine wrapper.
    """

    name = "URLChecker"

    def __init__(self, inQ, outQ, dbname=None, **kwargs):
        super(URLChecker, self).__init__(inQ, outQ, **kwargs)
        self.dbname = dbname

    def prepare_process(self):
        super(URLChecker, self).prepare_process()
        self.db = SetDB(self.dbname)

    def analyze(self, yzable):
        if not isinstance(yzable, FetchInfo):
            self.logger.debug("URLChecker got stray %r" % yzable)
            return None

        url = '%s%s' % (yzable.hostkey, yzable.relurl or '')
        if self.db.test_and_set(url):
            # Already in database
            self.logger.debug('Already in database: %r' % yzable)
            return None
        else:
            self.logger.debug('Newly added to database: %r' % yzable)
            return yzable

class HostWhitelist(Analyzer):
    """ Ensures that host is on whitelist, or rejects URL. """

    name = "HostWhitelist"

    def __init__(self, inQ, outQ, whitelist=None, **kwargs):
        super(HostWhitelist, self).__init__(inQ, outQ, **kwargs)
        self.whitelist = frozenset(l.strip() for l in open(whitelist))

    def analyze(self, yzable):
        assert isinstance(yzable, FetchInfo)

        hostname = yzable.hostkey.rsplit('/')[-1]
        if hostname in self.whitelist:
            return yzable
        else:
            return None


class HostBudgetHash(dict):
    """
    map from hostnames to budgets.
    We abstract away for future memory-saving implementation,
    but for now this is just a dict.
    """
    pass

class HostSpreader(Analyzer):
    """
    Spreads URLs from a host between various queues.

    This is where prioritization and fairness is done, by splitting between queues.
    """

    name = "HostSpreader"

    def __init__(self, inQ, outQ, qdir=None, host_budget=None, **kwargs):
        """
        @qdir is a directory with FIFO queues.
        @host_budget is a callable that takes a host and returns its budget.
        """

        super(HostSpreader, self).__init__(inQ, outQ, **kwargs)

        assert isinstance(qdir, str)
        assert callable(host_budget)

        self.qdir = qdir
        self.host_budget = host_budget

    def load_budget_hash(self, q):
        """ Store count of number of URLs from each host in queue. """
        h = HostBudgetHash()

        for fetch_rec in iter(q):
            h[fetch_rec.hostkey] = h.get(fetch_rec.hostkey, 0) + 1
        q.reset()

        self.logger.debug('load_budget_hash found %d hosts, '
                          'minimum %d to maximum %d occurences per host' % \
                              (len(h.keys()), min(h.values() or [0]), max(h.values() or [0])))

        return h

    def find_queues(self):
        """ Generator that yields queues in data path. """
        for i in os.listdir(self.qdir):
            path = os.path.join(self.qdir, i)
            if os.path.isdir(path):
                yield FetchInfoFIFO(path)

    def initialize_queues(self):
        """ Find all queues, and initialize them. """
        # First, load host budgets.
        queues = []

        for q in self.find_queues():
            queues.append((q, self.load_budget_hash(q)))
       
        return queues

    def prepare_process(self):
        super(HostSpreader, self).prepare_process()

        self.queues = self.initialize_queues()
        self.logger.debug('Initialized %d queues' % len(self.queues))

    def analyze(self, yzable):
        """
        We insert the analyzable into a queue here, and drop it from the chain.

        We add up to the host budget per queue, and then move to the next queue.
        """

        assert isinstance(yzable, FetchInfo)

        per_queue_budget = self.host_budget(yzable.hostkey)

        self.logger.debug('Host budget %d for: %r' % (per_queue_budget, yzable))

        # FIXME: do proper rotation.

        for q, budget_hash in self.queues[:-2]:
            budget_used = budget_hash.get(yzable.hostkey, 0)
            if budget_used < per_queue_budget:
                q.put(yzable)
                budget_hash[yzable.hostkey] = budget_used + 1
                q.sync()
                self.logger.debug('Added %r to queue' % yzable)
                return None

        # Goes into budget-exceeded queue.
        self.queues[-1][0].put(yzable)
        self.queues[-1][0].sync()
        self.logger.debug('Added %r to overflow queue' % yzable)
        return None

def init_host_queues(topdir, num=10):
    """ Make top-level directories for queues. """
    for i in range(num-1):
        try:
            os.makedirs(os.path.join(topdir, 'url-queue-%05d.d' % i))
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise

    # Call the dumping ground queue 99999 for now.
    try:
        os.makedirs(os.path.join(topdir, 'url-queue-%05d.d' % 99999))
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise

    return topdir

def get_new_link_queue_analyzerchain(debug = None,
                                     qdir = 'host_queues',
                                     dbname = 'urlchecker.db',
                                     whitelist = 'whitelist.txt',
                                     ):

    def return_one(*args, **kwargs):
        """ For host budgets, max one url from each host per queue. """
        return 1

    try:
        ac = AnalyzerChain(debug=debug)
        # FIXME: move configuration values to config file
        ac.append(HostWhitelist, 1, kwargs={'whitelist': whitelist})
        ac.append(URLChecker, 1, kwargs={'dbname': dbname})
        ac.append(HostSpreader, 1, kwargs={'qdir': init_host_queues(qdir),
                                           'host_budget': return_one})
        
        ac.start()
        return ac
    except:
        # FIXME: add logging.
        raise

