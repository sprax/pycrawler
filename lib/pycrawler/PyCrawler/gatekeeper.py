

"""
Functionality to limit which URLs we crawl.
"""

__copyright__ = "Copyright 2010, Nokia Corporation"
__license__ = "MIT License"
__version__ = "0.1"
__revision__ = "$Id$"

from robotexclusionrulesparser import RobotExclusionRulesParser

from analyzer_chain import Analyzer, FetchInfo
from url import get_parts

from PersistentQueue import SetDB

import sqlite3

class RobotsDB(dict):
    def __init__(self, filename='robots.db'):
        pass

class RobotsChecker(object):
    def __init__(self):
        self.robots = RobotsDB('robots.db')

    def check(self, hostkey, relurl):
        """ Return True if allowed to fetch, False if not, None
        if we do not have robots.txt for this entry. """

        robotstxt, expiration = self.robots.get(hostkey, (None, None))

        if robotstxt is None:
            return None

        # FIXME: mtime?  we need to let robots.txt expire.

        robotparser = RobotExclusionRulesParser()


        if robotsparser.is_expired():
            return None

        robotparser.seturl(hostkey + '/robots.txt')
        robotparser.parse(robotstxt.splitlines())
        return robotparser.can_fetch(hostkey + relurl)

class Gatekeeper(Analyzer):
    def __init__(self, inQ, outQ, robotsFetchQ=None, **kwargs):
        super(self, Gatekeeper).__init__(inQ, outQ, **kwargs)

        self.robotsFetchQ = robotsFetchQ
        self.fetchQ = fetchQ
        self.robots = RobotsChecker()

    def analyze(self, item):
        assert isinstance(item, FetchInfo)

        scheme, host, port, relurl = get_parts(item.hostkey + item.relurl)

        status = self.robots.check(item.hostkey, item.relurl)
        if status is None:
            self.robotsFetchQ.put(item)
            return None

        elif not status:
            self.logger.debug("%s failed robots check." % (item.hostkey + item.relurl))

        return item
