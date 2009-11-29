"""
Tests for PyCrawler.AnalyzerChain
"""
#$Id: $
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import sys
import time
sys.path.append("..")

from PyCrawler import URL

def test(*args, **kwargs):
    p1 = URL.packer()
    p1.add_url("http://test.host.com/long/long/%s;frag?foo=bar")
    o = p1.dump()
    p2 = URL.packer()
    p2.expand(o)
    assert o == p2.dump()

    import time
    num = 10000
    start = time.time()
    p = URL.packer()
    for i in xrange(num):
        p.add_url("http://test.host.com/long/long/%s;frag?foo=bar")
    end = time.time()
    print "packed %s URLs in %s seconds at a rate of %s create/sec" % \
        (num, end - start, num / (end - start))

    fh = open("URL_lists/u1000.h10")
    start = time.time()
    p = URL.packer()
    c = 0
    for u in fh.readlines():
        try:
            p.add_url(u.strip())
        except Exception, e:
            print str(e)
        c += 1
    end = time.time()
    print "packed %s URLs for %d hosts in %s seconds at a rate of %s create/sec" % \
        (c, len(p.hosts), end - start, num / (end - start))
    sys.exit()

if __name__ == "__main__":
    test()
