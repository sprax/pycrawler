"""

"""
# $Id$
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
__maintainer__ = "John R. Frank"


import heapq
import blist
import bisect

class IndexedPriorityQueue(object):
    """
    Maintains a persistent priority queue of python objects with an
    index into the queue based on a unique key attribute of the
    objects.

    Objects put into the queue can optionally have an update() method.
    If present in the objects of the queue, then instead of calling
    put(new_obj) to add items to the queue, one can also call
    update(new_obj).
    """
    def __init__(self, key_attr):
        """
        key_attr is the name of an attribute that the PriorityQueue
        will use as a unique identifier for objects in the queue.
        """
        _key_attr = key_attr
        _heap = blist.blist()
        _dict = {}

    def update(self, item):
        """
        Checks whether the queue already contains an object with the
        same key, and if so calls that object's update(item).
        """
        key = getattr(item, self._key_attr)
        if key in self._dict:
            self._dict[key].update(item)
            item = self._dict[key]
        else::
            self._dict[key] = item
        heapq.heappush(self._heap, item)

