# $Id: $
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import os
import sys
import cPickle
from random import random
from optparse import OptionParser

sys.path.insert(0, os.getcwd())
from PersistentQueue import nameddict, SafeStr

class MyND1(nameddict):
    _defaults = {"a": None, "b": None, "c": None}
    _key_ordering = ["b", "a", "c"]
    _val_types = [str, float, str]
    _sort_key = 1

class MyND2(nameddict):
    _key_ordering = ["b", "a", "c"]
    _val_types = [SafeStr, bool, str]

def test(SomeND, attrs):
    # verify that instances can be pickled
    d  = SomeND(attrs)
    d.c = "car"
    print d.__dict__
    p  = cPickle.dumps(d, -1)
    pd = cPickle.loads(p)
    print pd.__dict__
    assert d == pd, "failed because d = '%s' != '%s' = pd" % (repr(d), repr(pd))
    print "... pickling appears to work ... "

    s  = str(d)
    print "d.__dict__ = %s" % d.__dict__
    print "attempting to reconstruct fromstr(%s)" % s
    sd = SomeND.fromstr(s)
    print "reconstructed sd.__dict__ = %s" % sd.__dict__
    assert sd == d, "failed to reconstruct from %s" % repr(s)
    print "... collapsing to line of text works ..."

def sort_test():
    NDs = [MyND1({"a": .45}), MyND1({"a": .3}), MyND1({"a": .5})]
    for i in range(1000):
        NDs.append(MyND1({"a": random()}))
    unsorted = [str(ND) for ND in NDs]
    NDs.sort()
    sorted   = [str(ND) for ND in NDs]
    assert unsorted != sorted, "\nunsorted: %s\nsorted:   %s" % (unsorted, sorted)
    prev = 0
    for ND in sorted:
        ND = MyND1.fromstr(ND)
        assert prev <= ND.get_sort_val(), "wrong order: %s !<= %s" % (prev, ND.get_sort_val())
        prev = ND.get_sort_val()
    print "sorting test passed"

def serializing_test():
    NDs = [MyND1({"a": .45}), MyND1({"a": .3}), MyND1({"a": .5})]
    s = MyND1.dumps(NDs)
    NewNDs = MyND1.loads(s)
    assert NDs == NewNDs, "failed to 'loads' from 'dumps':\nNDs:    %s\nNewNDs: %s" % (NDs, NewNDs)
    print "loads(dumps) works"

if __name__ == "__main__":
    test(MyND1, {"a": 1.0, "b": None})
    test(MyND2, {"a": False, "b": "car|bomb"})

    sort_test()
    serializing_test()
