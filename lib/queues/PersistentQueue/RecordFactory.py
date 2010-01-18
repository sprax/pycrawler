#!/usr/bin/python2.6
"""
Simple tools for serializing and deserializing tuples of simple python
data types, and sorting flat-files and lists of these tuples.
"""
# $Id$
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
__maintainer__ = "John R. Frank"

import sys
import bz2
import keyword
import operator
import simplejson
import subprocess
from base64 import urlsafe_b64encode, urlsafe_b64decode
from Record import Record, define_record
from collections import namedtuple

class b64(object): 
    "indicates to dumps/loads to use urlsafe_b64encode/decode"
    pass

class JSON(object):
    "indicates to dumps/loads to serialize this as JSON"
    pass

Static = namedtuple("Static", "value")
"""
Static can be used in the template tuple passed to dumps/loads causing
the field to be serialized as the empty string ("").  When
deserialized, the value attribute of this instance is inserted into
that field of the tuple.
"""

def insort_right(sorted_list, record, key):
    """
    Insert 'record' while maintain sorted_list by comparing the values
    of 'key' field and performing bisect insort algorithm.  

    'key' should be an integer and is used as operator.itemgetter(key)
    """
    lo = 0
    hi = len(sorted_list)
    key = operator.itemgetter(key)
    rec_key = key(record)
    while lo < hi:
        mid = (lo + hi) // 2
        if rec_key < key(sorted_list[mid]):
            hi = mid
        else:
            lo = mid + 1
    sorted_list.insert(lo, record)

class RecordFactory(object):
    """
    Provides convenience methods for definiing and managing instances
    of Record with default values and for obtaining a 'template' for
    dumps/loads.
    """
    def __init__(self, record_class, template, defaults={}, delimiter="|"):
        """
        constructs _static_types used by dumps/loads
        
        'template' is a tuple of types aligned with _class.__slots__.

        (optional) 'defaults' is a dict that provides default values
        used by create().  Its keys must be names in _class.__slots__.

        'delimiter' is used by dumps/loads.
        """
        self._class = record_class
        self._template = template
        self._defaults = defaults
        self._delimiter = delimiter
        assert len(self._class.__slots__) == len(self._template), \
            "__slots__:  %s\ntemplate: %s" % \
            (self._class.__slots__, self._template)
        self._static_types = []
        for idx in range(len(self._template)):
            if isinstance(self._template[idx], Static):
                self._static_types.append(
                    (idx, self._class.__slots__[idx], 
                     self._template[idx].value))

    def create(self, *values, **attrs):
        """
        'values' must be either a dict or a sequence.  

        If a dict, then it will override the factory defaults before
        the factory's record type gets instantiated.

        If a sequence, it must have all the values needed to
        instantiate the record type.  No defaults will be applied.

        In all cases, the values of Static typed fields are enforced.
        """
        if attrs:
            _values = {}
            _values.update(self._defaults)
            _values.update(attrs)
            for idx, name, val in self._static_types:
                _values[name] = val
            return self._class(**_values)
        if values:
            # enforce Static types
            if self._static_types and values is not list:
                values = list(values)
            for idx, name, val in self._static_types:
                values[idx] = val
            return self._class(*values)

    def dumps(self, record):
        """
        Returns delimiter-separated string made from casting each field of
        'record' to a string.  'template' must be the same length as
        'record', and its fields will be used for applying rules below.

        If a component of 'template' is of type Static, then that component of
        'record' be serialized as an empty string.

        If a field in 'record' is None, but it has a type in 'template',
        then it will be serialized as an empty string.

        The serialization rules are:
         * any type, but value is None --> ""
         * Static --> ""
         * bool --> 0 or 1
         * float --> ten-digit fixed point number
         * b64 --> urlsafe_b64encode
         * JSON --> urlsafe_b64encode(bz2.compress(simplejson.dumps()))
         * others must be of type basestring already
        """
        assert len(record) == len(self._template), \
            "record and template must be same length"
        parts = []
        for idx in range(len(self._template)):
            val_type = self._template[idx]
            if isinstance(val_type, Static):
                parts.append("")
                continue
            val = record[idx]
            if val is None: 
                parts.append("")
            elif val_type is bool:
                parts.append(str(int(val)))
            elif val_type is int:
                parts.append(str(val))
            elif val_type is float:
                # ten-digit fixed-point rather than scientific notation,
                # so that sort doesn't think the "e" is a letter.
                parts.append("%.10f" % val)
            elif val_type is b64:
                parts.append(urlsafe_b64encode(val))
            elif val_type is JSON:
                parts.append(
                    urlsafe_b64encode(
                        bz2.compress(simplejson.dumps(val))))
            else:
                assert isinstance(val, basestring), \
                    "Is this a new special type: %s\nrepr: %s" % \
                    (type(val), repr(val))
                parts.append(val)
        return self._delimiter.join(parts)

    class BadFormat(Exception): 
        "raised by loads() when input string does not match factory"
        pass

    def loads(self, line):
        """
        Returns a tuple generated by splitting 'line' on factory's
        delimiter and applying the factory's types.  See notes on b64
        and Static above.
        """
        if not isinstance(line, basestring):
            raise self.BadFormat(
                "loads() expects basestring:\ntype: %s\nrepr: %s" % \
                    (type(line), repr(line)))
        try:
            parts = line.split(self._delimiter)
        except Exception, exc:
            raise self.BadFormat(
                "Failed to split on '%s':\nrepr: %s" %
                (self._delimiter, repr(line)))
        if len(parts) != len(self._template):
            raise self.BadFormat(
                "different lengths:\nline:     %s\ntemplate: %s" % \
                    (repr(line), repr(self._template)))
        record = []
        for idx in range(len(self._template)):
            val_type = self._template[idx]
            if isinstance(val_type, Static):
                record.append(val_type.value)
                continue
            val = parts[idx]
            if val is "" and val_type not in (str, unicode, basestring):
                record.append(None)
            elif val_type is bool:
                record.append(bool(int(val)))
            elif val_type is int:
                record.append(int(val))
            elif val_type is float:
                record.append(float(val))
            elif val_type is b64:
                record.append(urlsafe_b64decode(val))
            elif val_type is JSON:
                record.append(
                    simplejson.loads(
                        bz2.decompress(urlsafe_b64decode(val))))
            else:
                assert isinstance(val, basestring), \
                    "need a new special type?\ntype: %s\nrepr: %s" % \
                    (type(val), repr(val))
                record.append(val)
        return self._class(*record)

    def mergefiles(self, file_names, keys=(0,)):
        """
        file_names must be paths to files containing the result of
        calling dumps(<tuple>, template) on lists of tuples that have
        been sorted by their 'keys' fields.  'keys' is a sequence of
        integers specifying key positions and their order.  The sort
        order must be either numeric or alphanumeric depending on
        whether template[key] is of int/float or basestring.

        This assumes the files are sorted and simply merges them using
        the GNU coreutils sort function via subprocess.Popen

        This provides a generator for the merge sorted records, which
        yields tuples created by calling loads(<line>)
        """
        # args for subprocess, -m means merge pre-sorted files
        args = ["sort", "-m"]
        # define field separator
        args.append("-t%s" % self._delimiter)
        for key in keys:
            numerical = self._template[key] in (int, float) and "n" or ""
            args.append("-k%d%s" % (key + 1, numerical))
        # pass file names as args
        args += file_names
        #print " ".join(args)
        sort = subprocess.Popen(
            args=args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        # iterate over lines from sort
        while True:
            line = sort.stdout.readline()
            if not line: break
            # check for errors
            retcode = sort.poll()
            if retcode not in (None, 0):
                raise Exception("sort returned: %s, mesg: %s" % \
                                    (retcode, sort.stderr.read()))
            
            # remove newline from end of line
            assert line[-1] == "\n", "expected newline on every line"
            yield self.loads(line[:-1])

    def sort(self, records, keys=(0,), output_strings=True):
        """
        This serializes records and feeds them through the a
        subprocess.PIPE to GNU coreutils sort so that they become
        sorted by their 'keys' fields.  'keys' is a sequence of
        integers specifying key positions and their order.  The sort
        order will be either numeric or alphanumeric depending on
        whether template[key] is of int/float or basestring.

        This provides a generator for the sorted records. 

        If output_strings=True, this yields the serialized form of
        records, otherwise it yields tuples created by calling
        loads(<line>)
        """
        # args for subprocess, -m means merge pre-sorted files
        args = ["sort"]
        # define field separator
        args.append("-t%s" % self._delimiter)
        for key in keys:
            numerical = self._template[key] in (int, float) and "n" or ""
            args.append("-k%d%s" % (key + 1, numerical))
        #print " ".join(args)
        sort = subprocess.Popen(
            args=args,
            stdin =subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        # iterate over all records
        for rec in records:
            sort.stdin.write(self.dumps(rec) + "\n")
        # close stdin, so the sort begins
        sort.stdin.flush()
        sort.stdin.close()
        # iterate over output lines from sort
        while True:
            line = sort.stdout.readline()
            if not line: break
            # check for errors
            retcode = sort.poll()
            if retcode not in (None, 0):
                raise Exception("sort returned: %s, mesg: %s" % \
                                    (retcode, sort.stderr.read()))
            # remove newline from end of line
            assert line[-1] == "\n", "expected newline on every line"
            line = line[:-1]
            if not output_strings:
                line = self.loads(line)
            yield line

    @staticmethod
    def accumulate(records):
        """
        This example accumulator simply de-duplicates items.

        'records' must be iterable.

        Derived classes can (and should) overwrite this with a
        function that accumulates related records to produce single
        instances.
        """
        state = iter(records).next()
        for rec in records:
            if rec == state:
                continue
            else:
                # yield accumulated value, current becomes state
                yield state
                state = rec
        # previous state was last record, so yield it
        yield state

