"""
jrf@d3-3:~/pycrawler/lib$ python2.6  queues/tests/test_json.py 
simplejson module:  300000 in 1.485 --> 202058.9 rec/sec
json module:  300000 in 18.330 --> 16366.9 rec/sec
"""

import json
import simplejson

import random
from time import time

if __name__ == '__main__':
    size = 10**5
    num_serialized = 3
    total = num_serialized * size

    data = {}
    for x in random.sample(xrange(10**10), size):
        data[x] = random.random()

    start = time()
    for i in xrange(num_serialized):
        simplejson.loads(simplejson.dumps(data))
    elapsed = time() - start
    print "simplejson module:  %d in %.3f --> %.1f rec/sec" % (total, elapsed, total/elapsed)

    for i in xrange(num_serialized):
        json.loads(json.dumps(data))
    elapsed = time() - start
    print "json module:  %d in %.3f --> %.1f rec/sec" % (total, elapsed, total/elapsed)



