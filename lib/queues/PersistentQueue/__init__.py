#!/usr/bin/python2.6
"""
PersistentQueue provides FIFO and priority queue interfaces to a set
of flat files stored on disk.
"""
# $Id$
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
__maintainer__ = "John R. Frank"

from FIFO import FIFO
from Mutex import Mutex
from Records import RecordFactory, b64, Static, JSON, insort_right
from RecordFIFO import RecordFIFO
from BatchPriorityQueue import BatchPriorityQueue
