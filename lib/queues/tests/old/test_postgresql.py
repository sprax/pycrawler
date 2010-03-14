#!/usr/bin/python2.6
"""
Simple tools for serializing and deserializing tuples of simple python
data types, and sorting flat-files and lists of these tuples.
"""
# $Id$
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
__maintainer__ = "John R. Frank"

from time import time
from hashlib import md5
from random import random
from optparse import OptionParser

if __name__ == '__main__':
    import psycopg2

    parser = OptionParser()
    parser.add_option("-n", "--num", type=int, default=5, dest="num")
    (options, args) = parser.parse_args()

    conn = psycopg2.connect("dbname='pycrawler'") #user='geowebcrawler' host='localhost' password='geo123'")
    cur = conn.cursor()
    cur.execute("drop table hosts")
    cur.execute("""create table hosts(
hostname varchar unique, 
mbytes   int default 0, 
hits     int default 0, 
start    int default 0, 
next     int default 0)""")
    cur.execute("create index hosts_next on hosts(next)")
    conn.commit()
    conn.close()

    conn = psycopg2.connect("dbname='pycrawler'") #user='geowebcrawler' host='localhost' password='geo123'")
    def upsert(hostname, conn):
        cur = conn.cursor()
        cur.execute("select mbytes, hits, next from hosts where hostname=%s", (hostname,))
        rows = cur.fetchall()
        if not rows:
            cur.execute("insert into hosts values(%s)", (hostname,))
        else:
            [mbytes, hits, next] = rows[0]
            cur.execute("update hosts set next=%s where hostname=%s", (int(next + 100 * random()), hostname))
        conn.commit()

    start = time()
    for i in range(options.num):
        hostname = md5(str(i)).hexdigest()
        upsert(hostname, conn)
    elapsed = time() - start
    print "%d inserts in %.3f sec --> %.1f inserts/second" % (options.num, elapsed, options.num / elapsed)
    start = time()
    for i in range(options.num):
        hostname = md5(str(i)).hexdigest()
        upsert(hostname, conn)
    elapsed = time() - start
    print "%d updates in %.3f sec --> %.1f updates/second" % (options.num, elapsed, options.num / elapsed)

    conn.close()


