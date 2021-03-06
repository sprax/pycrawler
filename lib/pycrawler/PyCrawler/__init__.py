#!/usr/bin/env python2.6

"""
PyCrawler is python module that uses pycurl (libcurl) to provide a
high-throughput crawler that can scale to (probably) hundreds of
millions of pages and saturate large network pipes.  It implements
politeness and robots.txt checking, and provides a simple API for
creating your own content analyzers and link rankers for prioritizing
which links it follows.

See PyCrawler/license.txt
"""

__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank.  Copyright 2010, Nokia Corporation."
__license__ = "MIT License"
__version__ = "0.1"
__revision__ = "$Id$"

import url
from server import FetchServer, FetchClient, PORT, AUTHKEY, run_server
from fetcher import Fetcher
from analyzer_chain import AnalyzerChain, Analyzer, GetLinks, LogInfo, \
    SpeedDiagnostics, FetchInfo, FetchInfoFIFO
from crawl_state_manager import HostRecord
from new_link_queue import HostSpreader, URLChecker

# import PyCrawler logger into package namespace.
from _logging import logger
