# $Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import os
import sys
import Queue
import shutil
import cPickle
from random import random
from optparse import OptionParser

sys.path.insert(0, os.getcwd())
from PersistentQueue import nameddict, SafeStr, PersistentQueue

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

    # dumping a single item and reloading it:
    s = MyND1.dumps(d)
    pd = MyND1.loads(s)[0]
    assert d == pd and d.c == pd.c, "loads(dumps(d)) != d: %s -> %s -> %s" % (repr(str(d)), repr(str(s)), repr(str(pd)))
    print "... dumping a single item and reloading it works ..."

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

def storing_in_persistent_queue():
    # make it not sort
    MyND1._sort_key = None
    pq = PersistentQueue(data_test_dir, marshal=MyND1)
    NDs = [MyND1({"a": .45}), MyND1({"a": .3}), MyND1({"a": .5})]
    for ND in NDs:
        pq.put(ND)
        pq.sync()
    pq.close()
    pq = PersistentQueue("data_test_dir", marshal=MyND1)
    newNDs = []
    while 1:
        try:
            ND = pq.get()
        except Queue.Empty:
            break
        newNDs.append(ND)        
    pq.close()
    assert NDs == newNDs, \
        "failed to get the same thing back: \nNDs: [%s]\nnewNDs: [%s]" % \
        (", ".join([str(x) for x in NDs]),
         ", ".join([str(x) for x in newNDs]))
    print "passed the storage test"

def sorting_in_persistent_queue():
    MyND1._sort_key = 1
    pq = PersistentQueue(data_test_dir, marshal=MyND1)
    NDs = [MyND1({"a": .45}), MyND1({"a": .3}), MyND1({"a": .5})]
    for ND in NDs:
        pq.put(ND)
    print "calling sort"
    ret = pq.sort()
    assert ret != NotImplemented, "failed to setup nameddict for sorting"
    prev = 0
    newNDs = []
    while 1:
        try:
            ND = pq.get()
        except Queue.Empty:
            break
        newNDs.append(ND)
        assert prev <= ND.get_sort_val(), \
            "failed to get sorted order: %s !<= %s" % (prev, ND.get_sort_val())
        prev = ND.get_sort_val()
    NDs.sort()
    assert NDs == newNDs, \
        "failed to reconstruct the full sorted list: \nNDs: [%s]\nnewNDs: [%s]" % \
        (", ".join([str(x) for x in NDs]),
         ", ".join([str(x) for x in newNDs]))
    pq.close()
    print "passed sorting in persistent queue tests"

def merging(ELEMENTS=100, NUM_QUEUES=5):
    MyND1._sort_key = 1
    # create a bunch of random data in four queues
    NDs = []
    queues = []
    for i in range(NUM_QUEUES):
        pq = PersistentQueue(
            os.path.join(data_test_dir, str(i)), 
            marshal=MyND1)
        queues.append(pq)
        for i in range(ELEMENTS):
            ND = MyND1({"a": random()})
            NDs.append(ND)
            pq.put(ND)
    # use the first queue as the host, and merge all the queues into
    # the fourth queue
    ret = queues[0].sort(merge_from=queues) #, merge_to=queues[3])
    assert ret != NotImplemented, "failed to setup nameddict for sorting"
    #for i in range(5):
    #    print "%d has %d" % (i, len(queues[i]))
    prev = 0
    newNDs = []
    while 1:
        try:
            ND = queues[0].get()
        except Queue.Empty:
            break
        newNDs.append(ND)
        assert prev <= ND.get_sort_val(), \
            "failed to get sorted order: %s !<= %s" % (prev, ND.get_sort_val())
        prev = ND.get_sort_val()
    NDs.sort()
    for i in range(NUM_QUEUES * ELEMENTS):
        a = NDs.pop()
        b = newNDs.pop()
        assert a != b, "non-identical instances at %d: %s != %s" % (i, a, b)
    for pq in queues:
        pq.close()
    print "passed merging in persistent queue tests"
    
def rmdir(dir):
    if os.path.exists(dir):
        try:
            shutil.rmtree(dir)
        except Exception, exc:
            print "Did not rmtree the dir. " + str(exc)

data_test_dir = "data_test_dir"

if __name__ == "__main__":
    test(MyND1, {"a": 1.0, "b": None})
    test(MyND2, {"a": False, "b": "car|bomb"})

    sort_test()
    serializing_test()

    rmdir(data_test_dir)
    storing_in_persistent_queue()
    rmdir(data_test_dir)
    sorting_in_persistent_queue()
    rmdir(data_test_dir)
    merging()
    rmdir(data_test_dir)
