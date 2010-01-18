"""
Speed test of sorting unsorted files versus merging sorted files.
Just how much do we gain by sorting before writing each cache file in
a FIFO used in the BatchPriorityQueue?

jrf@d3-3:~/pycrawler/lib/queues$ python2.6 tests/test_mergesort.py -n 500000
500000 records sorted in 12.748 seconds --> 39221.0 records/second
500000 records sorted and then merged in 13.422 seconds --> 37252.3 records/second

The answer: slightly better to just sort once, so that's what we'll do
in BatchPriorityQueue
"""
#$Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import os
import sys
import blist
import Queue
import shutil
import random
import subprocess
from time import sleep, time
from optparse import OptionParser

sys.path.insert(0, os.getcwd())
import PersistentQueue

def log(msg):
    print msg
    sys.stdout.flush()

def rmdir(dir):
    if os.path.exists(dir):
        try:
            shutil.rmtree(dir)
        except Exception, exc:
            print "Did not rmtree the dir. " + str(exc)

def sort(dir, merge=False):
    # args for subprocess, -m means merge pre-sorted files
    args = ["sort", "-n"]
    if merge:
        args.append("-m")
    # pass file names as args
    args += [os.path.join(dir, x) for x in os.listdir(dir)]
    #print " ".join(args)
    sort = subprocess.Popen(
        args=args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    # iterate over lines from sort
    while True:
        line = sort.stdout.readline()
        if not line: break
        if line[-1] == "\n":
            line = line[:-1]
        # check for errors
        retcode = sort.poll()
        if retcode not in (None, 0):
            raise Exception("sort returned: %s, mesg: %s" % \
                                (retcode, sort.stderr.read()))
        yield line

parser = OptionParser()
parser.add_option("-n", "--num", type=int, default=5000, dest="num")
parser.add_option("-c", "--cache_size", type=int, default=5, dest="cache_size")
(options, args) = parser.parse_args()

num = options.num
assert num >= 1000, "must have larger num"

vals = blist.blist(random.sample(xrange(num * 1000), num))
random.shuffle(vals)

q_path = "test_dir"
o_path = "test_dir2"
rmdir(q_path)
rmdir(o_path)
start = time()
q = PersistentQueue.FIFO(q_path, cache_size=int(num/100))
for i in vals:
    q.put(str(i))
q.close()

o = PersistentQueue.FIFO(o_path)
for line in sort(os.path.join(q_path, "data")):
    #print line
    o.put(line)
elapsed_sort = time() - start

count = 0
assert len(vals) == len(o)
vals.sort()
for val in vals:
    ans = o.get()
    if len(ans) == 0:
        print "\ncount: %s, val: %s, but empty ans: %s" % (count, val, repr(ans))
        ans = o.get()
    ans = int(ans)
    assert val == ans, "\nval: %s\nans: %s\ncount: %s" % (val, ans, count)
    count += 1
assert len(o) == 0
o.close()
rmdir(q_path)
rmdir(o_path)

print "%d records sorted in %.3f seconds --> %.1f records/second" % (len(vals), elapsed_sort, len(vals) / elapsed_sort)


q_path = "test_dir"
o_path = "test_dir2"
rmdir(q_path)
rmdir(o_path)
temp = blist.blist()
random.shuffle(vals)
start = time()
q = PersistentQueue.FIFO(q_path, cache_size=int(num/100))
for i in vals:
    temp.append(i)
    if len(temp) == int(num/100):
        temp.sort()
        map(lambda x: q.put(str(x)), temp)
        temp = blist.blist()
q.close()

o = PersistentQueue.FIFO(o_path)
for line in sort(os.path.join(q_path, "data"), merge=True):
    o.put(line)
elapsed_merge = time() - start

count = 0
vals.sort()
assert len(vals) == len(o)
for val in vals:
    ans = int(o.get())
    assert val == ans, "\nval: %s\nans: %s\ncount: %s" % (val, ans, count)
    count += 1
assert len(o) == 0
o.close()
rmdir(q_path)
rmdir(o_path)

print "%d records sorted and then merged in %.3f seconds --> %.1f records/second" % (len(vals), elapsed_merge, len(vals) / elapsed_merge)

