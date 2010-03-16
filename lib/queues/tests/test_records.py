#!/usr/bin/python2.6
"""
tests for PersistentQueue/Records.py
"""
# $Id$
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
__maintainer__ = "John R. Frank"
import os
import sys
import copy
import blist
import shutil
import cPickle as pickle
import operator
from time import time
from random import random, sample, choice, shuffle
from hashlib import md5
from optparse import OptionParser

import PersistentQueue
from PersistentQueue import RecordFactory, b64, Static, JSON, insort_right

if __name__ == '__main__':

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

#class Answer(PersistentQueue.Record):
#    __slots__ = 'life', 'universe', 'everything'
#a1 = Answer(42, 42, lambda x: x.life - 5)
#print a1.everything

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

##### test mergefiles with one key
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
    records2 = [x for x in factory.mergefiles(file_names, keys=(2,))]
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

##### test mergefiles with three keys
# make 100 records with next, score, depth
    records = []
    for next in sample(xrange(10**9), 100):
        records.append(factory.create(**{
                    "next": next,
                    "score": int(random() * 10000),
                    "depth": int(random() * 10000)
                    }))
    shuffle(records)
# make ten files, fill them, and merge sort them
    file_names = []
    for i in range(10):
        fn = "test%d" % i
        file_names.append(fn)
        some_recs = records[10 * i : 10 * (i+1)]
        some_recs.sort(key=operator.itemgetter(1,2,0))
        some_recs = [factory.dumps(x) for x in some_recs]
        fh = open(fn, "w")
        fh.write("\n".join(some_recs))
        fh.close()
# do the merge sort
    records2 = [x for x in factory.mergefiles(file_names, keys=(1,2,0))]
# confirm sorted result
    merged = [(x.score, x.depth, x.next) for x in records2]
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
    records2 = [x for x in factory.accumulate(factory.mergefiles(file_names, keys=(2,)))]
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
    records = [factory.create(**{"depth": x, 
                                 "score": int(random() * 10**9),
                                 "next": int(random() * 10**9)}) 
               for x in sample(xrange(num * 1000), num)]
    shuffle(records)
    records2 = [x for x in factory.sort(records, keys=(1,0), output_strings=False)]
# they should be sorted
    merged = [(x.score, x.next) for x in records2]
    correct = copy.copy(merged)
    correct.sort()
    assert correct == merged, \
        "\ncorrect: %s\nmerged:  %s" % \
        (" ".join([str(x) for x in correct]),
         " ".join([str(x) for x in merged]))

##### test sort on strings
# make a factory for testing:
    MyRec = PersistentQueue.define_record("MyRec", ("docid", "hostid", "hostname", "relurl"))
    factory = RecordFactory(MyRec, (str, str, str, b64))
# copy the test data so we can test FIFO.__iter__ on it
    shutil.copytree("tests/url_parts", "url_parts_temp")
    UrlParts = PersistentQueue.define_record("UrlParts", "scheme hostname port relurl")
    f = PersistentQueue.RecordFIFO(UrlParts, (str, str, str, b64), "url_parts_temp")
# make records for all items in the FIFO
    records = [factory.create(*(
                md5("".join([u.scheme, "://", u.hostname, u.port, u.relurl])).hexdigest(),
                md5(u.hostname).hexdigest(),
                u.hostname, 
                u.relurl))
               for u in f]
    shutil.rmtree("url_parts_temp")
    shuffle(records)
    print "checking stream sorting on multiple, reverse-positioned hexadecimal keys"
    records2 = [x for x in factory.sort(records[:num], keys=(1,0), output_strings=False)]
    assert len(records2) == num, "\n\nlen(records2): %d\nnum:  %d" % (len(records2), num)
# they should be sorted by hostname, so check that first:
    merged = [x.hostid for x in records2]
    correct = copy.copy(merged)
    correct.sort()
    if not correct == merged:
        disagreements = []
        for idx in range(len(correct)):
            if correct[idx] != merged[idx]:
                disagreements.append((idx, correct[idx], merged[idx]))
        print "\n\nhostname ordering is wrong\ncorrect: %s\nmerged:  %s" % \
            (" ".join([str(x) for x in correct]),
             " ".join([str(x) for x in merged]))
        print "\n\n" + "\n".join([str(x) for x in disagreements])
        print "\n\n%d disagreements" % len(disagreements)
        sys.exit()
    docids = []
    hostnames = {}
    hostname = records2[0].hostname
    for x in records2:
        if hostname == x.hostname:
            docids.append(x.docid)
        else:
            hostnames[hostname] = hostnames.get(hostname, 0) + 1
            correct = copy.copy(docids)
            correct.sort()
            if not correct == docids:
                print "\ncorrect: %s\ndocids:  %s" % \
                (" ".join([str(y) for y in correct]),
                 " ".join([str(y) for y in docids]))
            hostname = x.hostname
            docids = []
    for hostname in hostnames:
        if hostnames[hostname] > 1:
            print "%d collisions of %s" % (hostnames[hostname], hostname)

    """

    this fails, probably because of chars in the hostnames... probably should b64 them.

# now do it with the hostname instead of hostid
    print "checking stream sorting on multiple, reverse-positioned alphanumeric keys"
    records2 = [x for x in factory.sort(records[:num], keys=(2,0), output_strings=False)]
    assert len(records2) == num, "\n\nlen(records2): %d\nnum:  %d" % (len(records2), num)
# they should be sorted by hostname, so check that first:
    merged = [x.hostname for x in records2]
    correct = copy.copy(merged)
    correct.sort()
    if not correct == merged:
        disagreements = []
        for idx in range(len(correct)):
            if correct[idx] != merged[idx]:
                disagreements.append((idx, correct[idx], merged[idx]))
        print "\n\nhostname ordering is wrong\ncorrect: %s\nmerged:  %s" % \
            (" ".join([str(x) for x in correct]),
            " ".join([str(x) for x in merged]))
        print "\n\n" + "\n".join([str(x) for x in disagreements])
        print "\n\n%d disagreements" % len(disagreements)
        sys.exit()
    docids = []
    hostnames = {}
    hostname = records2[0].hostname
    for x in records2:
        if hostname == x.hostname:
            docids.append(x.docid)
        else:
            hostnames[hostname] = hostnames.get(hostname, 0) + 1
            correct = copy.copy(docids)
            correct.sort()
            if not correct == docids:
                print "\ncorrect: %s\ndocids:  %s" % \
                (" ".join([str(y) for y in correct]),
                 " ".join([str(y) for y in docids]))
            hostname = x.hostname
            docids = []
    for hostname in hostnames:
        if hostnames[hostname] > 1:
            print "%d collisions of %s" % (hostnames[hostname], hostname)
    """
