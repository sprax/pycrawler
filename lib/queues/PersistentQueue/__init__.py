"""
PersistentQueue provides queue and heap-like interfaces to a set of
flat files stored on disk.
"""
# $Id: $
__copyright__ = "Copyright 2009, John R. Frank"
__credits__ = ["Kjetil Jacobsen"]
__license__ = "MIT License"
__version__ = "0.1"
__maintainer__ = "John R. Frank"

import LineFiles
from nameddict import nameddict, SafeStr
from PersistentQueue import PersistentQueue as Queue
from PersistentQueue import NotYet
from TriQueue import TriQueue, Blocked, Syncing, ReadyToSync
