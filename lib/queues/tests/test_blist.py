
import blist
from time import time
from random import sample
from optparse import OptionParser

parser = OptionParser()
parser.add_option("-n", "--num", type=int, default=5, dest="num")
(options, args) = parser.parse_args()

print "start with dict"
b = {}
start = time()
for i in sample(xrange(1000 * options.num), options.num):
    b[i] = 1
elapsed = time() - start
print "%d inserts in %.3f seconds --> %.1f inserts per second" % (len(b), elapsed, len(b) / elapsed)

exists = []
start = time()
for i in sample(xrange(1000 * options.num), options.num):
    exists.append(i in b)
elapsed = time() - start
trues = 0
falses = 0
for val in exists:
    if val:
        trues += 1
    else:
        falses += 1
print "%d lookups in %.3f seconds --> %.1f lookups per second" % (len(exists), elapsed, len(exists) / elapsed)
print "%d found, %d not found" % (trues, falses)

print "\n\nnow try blist"
b = blist.blist()
start = time()
for i in sample(xrange(1000 * options.num), options.num):
    b.append(i)
elapsed = time() - start
print "%d inserts in %.3f seconds --> %.1f inserts per second" % (len(b), elapsed, len(b) / elapsed)

exists = []
start = time()
for i in sample(xrange(1000 * options.num), options.num):
    exists.append(i in b)
elapsed = time() - start
trues = 0
falses = 0
for val in exists:
    if val:
        trues += 1
    else:
        falses += 1
print "%d lookups in %.3f seconds --> %.1f lookups per second" % (len(exists), elapsed, len(exists) / elapsed)
print "%d found, %d not found" % (trues, falses)

print
exists = sample(b, int(options.num / 10))
start = time()
for i in exists:
    b.remove(i)
elapsed = time() - start
print "%d removes in %.3f seconds --> %.1f removes per second" % (len(exists), elapsed, len(exists) / elapsed)

