#!/usr/bin/env python

"""
Run PyCrawler as a standalone program.
"""

__author__ = "John R. Frank"
__copyright__ = "Copyright 2010, Nokia Corporation."
__license__ = "MIT License"
__version__ = "0.1"
__revision__ = "$Id$"

import sys
from server import run_server

run_server(sys.argv)
