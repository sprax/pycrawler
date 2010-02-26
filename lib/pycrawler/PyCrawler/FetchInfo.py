#!/usr/bin/python2.6
""" A placeholder object for FetchInfo. """

__license__ = "MIT License"
__version__ = "0.1"
__revision__ = "$Id$"

from AnalyzerChain import Analyzable
import URL

class FetchInfo(Analyzable):
    def __init__(self, url=None, raw_data=None, depth=None,
                 start=None, end=None, state=None):
        scheme, hostname, port, self.relurl = URL.get_parts(url)
        self.hostkey = '%s://%s' % (scheme, hostname)
        if port:
            self.hostkey = self.hostkey + ':%s' % port
        self.relurl = relurl

        self.raw_data = raw_data
        self.depth = depth
        self.start = start
        self.end = end
        self.state = state

        self.links = []
