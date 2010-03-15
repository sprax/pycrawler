"""
Tests for PyCrawler.AnalyzerChain
"""
#$Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import os
import sys
import time
import traceback

from PyCrawler import url as URL

def tests():
    "tests for get_links"
    fake_doc = """
<a href="/foo1">1</a>
<a href="../foo2">2</a>
<a href="./foo3">3</a>
<a href="path1//foo4">4</a>
<a href="path2/..//path3/path4/../../path5//foo5/">4</a>
"""
    host = "https://crazyhost.com"
    errors, links = URL.get_links(host, "/dog/", fake_doc)
    #pprint.pprint(errors)
    #pprint.pprint(links)
    assert host == "".join((links[0][0], "://", links[0][1])), "links[0]: %s" % str(links[0])
    relurls = [x[3] for x in links]
    expected_relurls = [
        "/foo1",
        "/foo2",
        "/dog/foo3",
        "/dog/path1/foo4",
        "/dog/path5/foo5/",
        ]
    wrong = False
    for expected in expected_relurls:
        if expected not in relurls:
            print "Failed to find %s" % expected
            wrong = True
    if wrong:
        raise Exception("Test failed for reasons above")
    else:
        print "Test passed"

if __name__ == "__main__":
    import sys
    import pprint
    from optparse import OptionParser
    parser = OptionParser(usage="",
                           description=__doc__)
    parser.add_option("--host",     dest="host",   default="",    help="")
    parser.add_option("--relurl",   dest="relurl", default="",    help="")
    parser.add_option("--file",     dest="file",   default="",    help="")
    parser.add_option("--test",     dest="test",   default=False, action="store_true",  help="Run tests for get_links")
    (options, args)= parser.parse_args()
    if options.test:
        tests()
        sys.exit(0)

    errors, links = URL.get_links(options.host, options.relurl, open(options.file).read())
    if errors:
        print "Got errors:"
        print pprint.pprint(errors)
    print links
