"""
PyCrawler is python module that uses pycurl (libcurl) to provide a
high-throughput crawler that can scale to (probably) hundreds of
millions of pages and saturate large network pipes.  It implements
politeness and robots.txt checking, and provides a simple API for
creating your own content analyzers and link rankers for prioritizing
which links it follows.

See PyCrawler/license.txt
"""
#$Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import URL
from Fetcher import Fetcher, URLinfo
from Logging import ChangeLoggerContainer
from AnalyzerChain import AnalyzerChain, Analyzer
from TextProcessing import is_text
