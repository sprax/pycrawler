#!/usr/bin/python2.6
"""
Simple persistent FIFO storage wrapped in a RecordFactory
"""
# $Id$
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
__maintainer__ = "John R. Frank"

from fifo import FIFO
from record_factory import RecordFactory

class RecordFIFO(RecordFactory, FIFO):
    "Simple persistent FIFO storage wrapped in a RecordFactory"
    def __init__(self, record_class, template, data_path,
                 defaults={}, delimiter="|", cache_size=2**16):
        FIFO.__init__(self, data_path, cache_size)
        RecordFactory.__init__(
            self, record_class, template, defaults, delimiter)
    
    def put(self, *values, **attrs):
        """
        If values is a namedtuple, then it is used as the record.
        Otherwise, a record is created from values or attrs.

        This then serializes record before putting it in FIFO.
        """
        if not (values and isinstance(values, self._class)):
            record = self.create(*values, **attrs)
        else:
            record = values
        FIFO.put(self, self.dumps(record))

    def next(self):
        "uses next() line from FIFO to return deserialized record"
        return self.loads(FIFO.next(self))

    def get(self):
        "gets line from FIFO and returns deserialized record"
        return self.loads(FIFO.get(self))
