"""
nameddict implementation that can be pickled and also provides __str__
and fromstr() methods that enabled ordered single-line
serialization/deserialization for use in sorting flat files containing
many string representations of these objects.
"""
#$Id$
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
import sys
import copy
import operator
from base64 import urlsafe_b64encode, urlsafe_b64decode
"""
TODO: 

 * Perhaps there is a better way to do this using collections?

 * use __slots__

 * do something cleaner with SafeStr, so that fromstr() does not have
   to know about urlsafe_b64decode

 * more tests...
"""

class SafeStr:
    """
    A wrapper around urlsafe_b64decode/encode for stuffing rich
    strings into our string formatting delimited by "|".  Any string
    that might contain the delimiter or another character that could
    create newlines etc should be given this type in _val_types
    """
    def __init__(self, unsafe_str):
        """
        Take a string that might have characters that mess with our
        single-line output formatting and base64 encode it.
        """
        self.safe_str = urlsafe_b64encode(unsafe_str)
    def __str__(self):
        """
        Return the safe form of the string.
        """
        return self.safe_str
    def __repr__(self):
        """
        Give back the unsafe string, which can be passed into __init__
        """
        return urlsafe_b64decode(self.safe_str)

class nameddict(dict):
    """
    simple nameddict implementation that can be pickled
    """
    # Subclasses can set this to a dictionary of default attr keys and
    # values to use for all instances.  The attrs passed into __init__
    # override this.
    _defaults = None

    # Subclasses must set this to be a list of attr names, so that the
    # __str__ and fromstr() methods can work.
    _key_ordering = None

    # Subclasses also must set _val_types to be a list of built-in
    # types, e.g. float, int, str, in the order of key_ordering
    _val_types = None

    # Subclasses can set this to an integer indicating a key in
    # _key_ordering, the value of which will be used as the sorting
    # key for the class in comparison operators.  See __lt__, __gt__,
    # __eq__, etc.
    _sort_key = None

    # used in (de)serializing and sorting
    DELIMITER = "|"

    def __init__(self, attrs=None):
        """
        Sets the instances attributes to the keys/values in attrs, and
        falls back to the class attribute _defaults for any keys not
        specified in attrs.
        """
        dict.__init__(self)
        # now use defaults if defined:
        if self._defaults is not None:
            # do not change class attribute, just a copy of it
            defaults = copy.copy(self._defaults)
            if attrs is not None:
                defaults.update(attrs)
            attrs = defaults
        # Now update __dict__, which Python made for us and determines
        # the attributes of this instance.
        if attrs is not None:
            self.__dict__.update(attrs)

    def __getinitargs__(self):
        return (self.__dict__,)

    def get_val_vector(self):
        """
        Returns a list of string representations of attributes of this
        instance.  Ordered by self._key_ordering
        """
        parts = []
        for attr_num in range(len(self._key_ordering)):
            param = self._key_ordering[attr_num]
            val_type = self._val_types[attr_num]
            val = getattr(self, param)
            if val is None: 
                val = ""
            elif val_type is bool:
                # force booleans to integers, so we can de-string
                # easily in fromstr
                val = int(val)
            elif val_type is float:
                # limit precision to ten digits and force fixed-point
                # rather than scientific notation, so that sort
                # doesn't think the "e" is a letter.
                val = "%.10f" % val
            else:
                val = val_type(val)
            val = str(val)
            parts.append(val)
        return parts

    def __str__(self):
        """
        Returns DELIMITER-separated string made from get_val_vector
        """
        line = self.DELIMITER.join(self.get_val_vector())
        return line

    class BadFormat(Exception): pass

    @classmethod
    def fromstr(cls, line):
        """
        Returns a nameddict instance constructed by parsing 'line',
        which must be in the same format as created by __str__.
        """
        assert isinstance(line, basestring), \
            "fromstr() called with %d instead of basestring: %s" % \
            (type(line), repr(line))
        assert cls._key_ordering is not None, \
            "fromstr() requires subclass to define _key_ordering"
        try:
            parts = line.split(cls.DELIMITER)
        except Exception, exc:
            raise cls.BadFormat(
                "Failed to split on '%s' this line: %s" % 
                (cls.DELIMITER, repr(line)))
        if not len(parts) == len(cls._key_ordering):
            raise cls.BadFormat(
                "error in:%s\nlen(parts) = %d differs from %d\nparts: %s\n_key_ordering: %s" %
                (line, len(parts), len(cls._key_ordering), parts, cls._key_ordering))
        attrs = {}
        for attr_num in range(len(cls._key_ordering)):
            param = cls._key_ordering[attr_num]
            val_type = cls._val_types[attr_num]
            str_val = parts[attr_num]
            attrs[param] = cls.reconstitute(str_val, val_type)
        return cls(attrs)

    @staticmethod
    def reconstitute(str_val, val_type):
        """
        Converts str_val into the python type indicated by val_type,
        and handles a few special cases related to stringifying None,
        bool, and SafeStr.
        """
        if str_val is "":
            val = None
        elif val_type is bool:
            val = int(str_val)
            val = val_type(val)
        elif val_type is SafeStr:
            val = urlsafe_b64decode(str_val)
        else:
            val = val_type(str_val)
        return val

    def get_sort_val(self):
        """
        Returns the reconstituted value of the sort_key attribute of
        this instance.
        """
        if self._sort_key is None:
            return None
        val_type = self._val_types[self._sort_key]
        param = self._key_ordering[self._sort_key]
        str_val = getattr(self, param)
        return self.reconstitute(str_val, val_type)

    def same_type(self, other):
        """
        Returns bool indicating whether or not other is the same
        subclass of nameddict and setup in the same way, although
        possibly with different values.
        """
        return (type(self) == type(other) and 
                self._key_ordering == other._key_ordering and 
                self._sort_key == other._sort_key and
                self._val_types == other._val_types)

    def __lt__(self, other):
        "Rich comparison operator based on _sort_key"
        if not self.same_type(other): return NotImplemented
        return self.get_sort_val() < other.get_sort_val()

    def __le__(self, other):
        "Rich comparison operator based on _sort_key"
        if not self.same_type(other): return NotImplemented
        return self.get_sort_val() <= other.get_sort_val()

    def __gt__(self, other):
        "Rich comparison operator based on _sort_key"
        if not self.same_type(other): return NotImplemented
        return self.get_sort_val() > other.get_sort_val()

    def __ge__(self, other):
        "Rich comparison operator based on _sort_key"
        if not self.same_type(other): return NotImplemented
        return self.get_sort_val() >= other.get_sort_val()

    def __eq__(self, other):
        "Rich comparison operator based on _sort_key"
        if not self.same_type(other): return NotImplemented
        return self.get_sort_val() == other.get_sort_val()

    def __ne__(self, other):
        "Rich comparison operator based on _sort_key"
        if not self.same_type(other): return NotImplemented
        return self.get_sort_val() != other.get_sort_val()

    @classmethod
    def accumulator(cls, acc_state, line):
        """
        This example accumulator simply de-duplicates items.
        
        Derived classes can (and should) overwrite this with a
        function that accumulates related records after a sort of
        their serialized form and produces a single serialized
        instance.  

        accumulator functions take two arguments and return to values:
       
            * first argument is previous first return value, or None
              for the first pass of accumulation.

            * second argument is the next item to accumulate

            * first return value is current state of accumulation

            * second return value is None unless the accumulator is
              done accumulating items into a single item

        If 'line' has zero length, then the previous set of rows was
        the last batch, and the first return value should be None,
        signaling to the sort function that it should break out of
        reading.  The second return value should be the last
        accumulated row.
        """
        if line == "":
            # previous state was last record, so cause break
            return None, cls.dumps(acc_state)
        current = cls.loads(line)
        if acc_state is None:
            # first pass accumulation
            return current, None
        if current == acc_state:
            # same as previous, so ignore this one:
            return acc_state, None
        else:
            # new one! give back a serialized form as second value,
            # and 'current' becomes the acc_state:
            return current, cls.dumps(acc_state)

    @classmethod
    def dumps(cls, items):
        """
        'items' can be either a single instance or a list of instances
        of nameddict or properly constructed subclasses.  If a list,
        then this serializes each item, joins with newlines, appends a
        newline at the end, and returns the string.

        Sorting is handled using the rich comparison methods of the
        nameddict class, which rely on _sort_key

        Each item is treated as a separate entity to serialize using its
        __str__ method.
        """
        if not isinstance(items, list):
            items = [items]
        if cls._sort_key is not None:
            items.sort()
        return "\n".join([str(x) for x in items]) + "\n"

    @classmethod
    def dump(cls, items, output):
        """
        serializes items using the staticmethod dumps, and writes it
        to the open 'output' file object.
        """
        output.write(cls.dumps(items))

    @classmethod
    def load(cls, input):
        """
        Loads into memory the full contents of the (already open) 'input'
        file object.  Calls loads on the loaded contents.

        Returns an array of instances of this class.
        """
        return cls.loads(input.read())

    @classmethod
    def loads(cls, input):
        """
        'input' is an in-memory string.  Treats each line in 'input'
        as a separate entity to deserialize using this class' fromstr
        method.

        Returns either a list of multiple items, or a single item.
        """
        ret = []
        for line in input.splitlines():
            if line:
                ret.append(cls.fromstr(line))
        if len(ret) == 1:
            ret = ret[0]
        return ret
