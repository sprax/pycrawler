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

DELIMITER = "|"

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

class nameddict:
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

    def __init__(self, attrs=None, key_ordering=None, val_types=None):
        """
        Override the values of class attribute:

            attrs overrides _defaults

            key_ordering overrides _key_ordering
            
            val_types overrides _val_types

        """
        # do the simple overrides first
        if key_ordering is not None:
            self._key_ordering = key_ordering
        if val_types is not None:
            self._val_types = val_types
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
        # For pickling to work, the __module__ variable needs to be
        # set to the frame where the named dict is created.  Bypass
        # this step in enviroments where sys._getframe is not defined
        # (Jython for example) or sys._getframe is not defined for
        # arguments greater than 0 (IronPython).
        try:
            self.__module__ = sys._getframe(1).f_globals.get("__name__", "__main__")
        except (AttributeError, ValueError):
            pass

    def __getinitargs__(self):
        return (self.__dict__, self._key_ordering, self._val_types)

    def __eq__(self, other):
        if not isinstance(other, nameddict):
            return NotImplemented
        if not self._defaults == other._defaults:
            return False
        if not self._key_ordering == other._key_ordering:
            return False
        if not self._val_types == other._val_types:
            return False
        for key in self._key_ordering:
            if getattr(self, key) != getattr(other, key):
                return False
        return True

    def __str__(self):
        """
        Provides a string of DELIMITER-separated string
        representations of attrs of this instance.  Ordered by
        self._key_ordering
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
            else:
                val = val_type(val)
            val = str(val)
            parts.append(val)
        line = DELIMITER.join(parts)
        return line

    class BadFormat(Exception): pass

    def fromstr(self, line):
        """
        Returns a nameddict instance constructed by parsing 'line',
        which must be in the same format as created by __str__.
        """
        assert self._key_ordering is not None, \
            "fromstr() requires subclass to define _key_ordering"
        try:
            parts = line.split(DELIMITER)
        except Exception, exc:
            raise self.BadFormat(
                "Failed to split on '%s' this line: %s" % 
                (DELIMITER, repr(line)))
        if not len(parts) == len(self._key_ordering):
            raise self.BadFormat(
                "len(parts) = %d differs from %d" %
                (len(parts), len(self._key_ordering)))
        attrs = {}
        for attr_num in range(len(self._key_ordering)):
            param = self._key_ordering[attr_num]
            val_type = self._val_types[attr_num]
            str_val = parts[attr_num]
            if str_val is "":
                val = None
            elif val_type is bool:
                val = int(str_val)
                val = val_type(val)
            elif val_type is SafeStr:
                val = urlsafe_b64decode(str_val)
            else:
                val = val_type(str_val)
            attrs[param] = val
        return self.__new__(attrs)

    def __new__(self, attrs):
        return nameddict(attrs, self._key_ordering, self._val_types)

def test(attrs, key_ordering, val_types):
    # verify that instances can be pickled
    from cPickle import loads, dumps
    d  = nameddict(attrs, key_ordering, val_types)
    d._key_ordering.append("c")
    d._val_types.append(str)
    d.c = "car"
    p  = dumps(d, -1)
    pd = loads(p)
    print d.__dict__
    print pd.__dict__
    assert d == pd, "failed because d = '%s' != '%s' = pd" % (repr(d), repr(pd))
    print "... pickling appears to work ... "

    s  = str(d)
    factory = nameddict(key_ordering=key_ordering, val_types=val_types)
    sd = factory.fromstr(s)
    print sd.__dict__
    assert sd == d, "failed to reconstruct from %s" % repr(s)
    print "... collapsing to line of text works ..."

if __name__ == "__main__":
    test({"a": 1.0, "b": None}, ["b", "a"], [str, float])
    test({"a": False, "b": "car|bomb"}, ["b", "a"], [SafeStr, bool])
