#!/usr/bin/python2.6
"""
tests for PersistentQueue/Records.py
"""
# $Id: $
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
__maintainer__ = "John R. Frank"
import os
import sys
import copy
import blist
import cPickle as pickle
import operator
from time import time
from random import random, sample, choice, shuffle
from hashlib import md5
from optparse import OptionParser

sys.path.insert(0, os.getcwd())
import PersistentQueue
from PersistentQueue import RecordFactory, b64, Static, JSON, insort_right

parser = OptionParser()
parser.add_option("-n", "--num", type=int, default=5, dest="num")
(options, args) = parser.parse_args()

##### check Record creation
class Point(PersistentQueue.Record):
    __slots__ = 'x', 'y'

p = Point(3, 4)
p = Point(y=5, x=2)
p = Point(-1, 42)
assert (p.x, p.y) == (-1, 42), str((p.x, p.y))
x, y = p
assert (x, y) == (-1, 42), str((x, y))

class Badger(PersistentQueue.Record):
    __slots__ = 'large', 'wooden'
badger = Badger('spam', 'eggs')
import cPickle as pickle
assert repr(pickle.loads(pickle.dumps(badger))) == repr(badger)

class Answer(PersistentQueue.Record):
    __slots__ = 'life', 'universe', 'everything'
a1 = repr(Answer(42, 42, 42))
a2 = eval(a1)
assert repr(a2) == a1, str((repr(a2), a1))

a2.life = 37
ra2 = repr(a2)
a3 = eval(ra2)
assert repr(a3) == ra2, str((repr(a3), ra2))
assert a3.life == 37

Dog = PersistentQueue.define_record("Dog", ("legs", "name"))
d = Dog(legs=4, name="barny")
assert repr(d) == """Dog(legs=4, name='barny')""", repr(d)

assert repr(pickle.loads(pickle.dumps(d))) == repr(d)

##### Static is not broken
hi = Static("hi")
assert isinstance(hi, Static), type(hi)
assert hi.value == "hi"    

# make a factory for testing:
MyRec = PersistentQueue.define_record("MyRec", ("next", "score", "depth", "data", "hostkey", "foo", "dog"))
factory = RecordFactory(
    MyRec,
    (int, float, int, b64, Static("http://www.wikipedia.com"), Static(None), JSON),
    {"next": 0, "score": 0, "data": "did we survive b64ing?", "dog": {}})

rec = factory.create(**{"depth": -100})
s = pickle.dumps(rec)
rec2 = pickle.loads(s)
assert rec == rec2, "\n\nunpickled: %s\nrec:       %s" % (repr(rec2), repr(rec))

##### check bisect insort routine
# make a sorted list of records
records = blist.blist()
num = options.num
upto_hundred = range(num)
key = 2
start = time()
for val in sample(upto_hundred, num):
    vals = (choice(upto_hundred), random(), val, 
            md5(str(random())).hexdigest(), "not_wikipedia", "not_None", {1: "car"})
    rec = factory.create(*vals)
    insort_right(records, rec, key)

for val in sample(upto_hundred, num):
    rec = factory.create(**{"depth": val})
    insort_right(records, rec, key)
elapsed_insort = time() - start
# compare with sort
_records = copy.copy(records)
shuffle(_records)
records2 = blist.blist()
start = time()
for rec in _records:
    records2.append(rec)
    # could move this next line *inside* the for loop to make it
    # resort on every insert
records2.sort(key=operator.itemgetter(key))
elapsed_sort = time() - start
# check structure before reporting speed, in case is wrong
assert [x.depth for x in records] == [x.depth for x in records2], \
    "\nrecords:  %s\n\nrecords2: %s" % (records, records2)
assert records2[0].hostkey is "http://www.wikipedia.com", records2[0].hostkey
assert records2[0].foo is None, records2[0].foo

##### report speed tests
print "%d records insort: %.3f (%.1f per sec), sort: %.3f (%.1f per sec)" % \
    (len(records), elapsed_insort, len(records) / elapsed_insort,
     elapsed_sort, len(records2) / elapsed_sort)

# with only one sort, after all inserts
#600000 records insort: 46.129 (13006.9 per sec), sort: 7.126 (84201.4 per sec)

# with re-sort on every insert, to maintain sorted order
#6000 records insort: 0.230 (26104.1 per sec), sort: 17.482 (343.2 per sec)

##### test writing of records using dumps/loads
shuffle(records)
test_file = "test_sorttuples.txt"
f = open(test_file, "w")
f.write("\n".join((factory.dumps(x) for x in records)))
f.close()
f = open(test_file, "r")
records2 = blist.blist([factory.loads(x) for x in f.read().splitlines()])
f.close()
# now that we have reconstructed the list, sort it for comparison
records2.sort(key=operator.itemgetter(key))
# sort for comparison below
records.sort(key=operator.itemgetter(key))
# confirm sorted result
assert [x.depth for x in records] == [x.depth for x in records2], \
    "\nrecords:  %s\n\nrecords2: %s" % (records, records2)
assert records2[0].hostkey is "http://www.wikipedia.com", records2[0].hostkey
assert records2[0].foo is None, records2[0].foo
os.remove(test_file)

##### test mergefiles
# make five files, fill them, and merge sort them
file_names = []
for i in range(5):
    fn = "test%d" % i
    file_names.append(fn)
    some_recs = records[4 * i : 4 * (i+1)]
    some_recs.sort(key=operator.itemgetter(2))
    some_recs = [factory.dumps(x) for x in some_recs]
    fh = open(fn, "w")
    fh.write("\n".join(some_recs))
    fh.close()
# do the merge sort
records2 = [x for x in factory.mergefiles(file_names, key=2)]
# confirm sorted result
merged = [x.depth for x in records2]
correct = copy.copy(merged)
correct.sort()
assert correct == merged, \
    "\ncorrect: %s\nmerged:  %s" % \
    (" ".join([str(x) for x in correct]),
     " ".join([str(x) for x in merged]))
# cleanup
for fn in file_names:
    os.remove(fn)

##### test accumulate
# make fifty records of only five types
records = []
for i in range(5):
    records += [factory.create(**{"depth": i}) for x in range(10)]
# shuffle them and write into ten files:
shuffle(records)
file_names = []
for i in range(10):
    fn = "test%d" % i
    file_names.append(fn)
    some_recs = records[5 * i : 5 * (i+1)]
    some_recs.sort(key=operator.itemgetter(2))
    some_recs = [factory.dumps(x) for x in some_recs]
    fh = open(fn, "w")
    fh.write("\n".join(some_recs))
    fh.close()
# accumulate them using the simple deduplicator above:
records2 = [x for x in factory.accumulate(factory.mergefiles(file_names, key=2))]
# should get only five out:
assert len(records2) == 5, "\n".join([factory.dumps(x) for x in records2])
# and they should be sorted
merged = [x.depth for x in records2]
correct = copy.copy(merged)
correct.sort()
assert correct == merged, \
    "\ncorrect: %s\nmerged:  %s" % \
    (" ".join([str(x) for x in correct]),
     " ".join([str(x) for x in merged]))
# cleanup
for fn in file_names:
    os.remove(fn)

##### test sort
# make fifty records of only five types
records = [factory.create(**{"depth": x}) for x in sample(xrange(num * 1000), num)]
shuffle(records)
records2 = [x for x in factory.sort(records, key=2, output_strings=False)]
# they should be sorted
merged = [x.depth for x in records2]
correct = copy.copy(merged)
correct.sort()
assert correct == merged, \
    "\ncorrect: %s\nmerged:  %s" % \
    (" ".join([str(x) for x in correct]),
     " ".join([str(x) for x in merged]))
