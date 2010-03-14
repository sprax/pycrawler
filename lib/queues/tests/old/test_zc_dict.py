"""
Simple tests of the speed of Zope Corp's persistent dictionaries,
which use BTree.
"""
import transaction
from time import time
from ZODB import FileStorage, DB
from zc.dict import Dict

if __name__ == '__main__':
    big_state = Dict()

    num = 10**9
    start = time()
    for i in range(num):
        big_state[i] = "boom"
    end = time()
    elapsed = end - start
    print "inserted %d records in %.2f seconds: %.2f rec/sec" % (num, elapsed, num / elapsed)

    start = time()
    storage = FileStorage.FileStorage('ZODB-test.fs')
    db = DB(storage)
    connection = db.open()
    root = connection.root()
    root["big_state"] = big_state
    transaction.manager.commit()
    connection.close()
    end = time()
    elapsed = end - start
    print "saved %d records in %.2f seconds: %.2f rec/sec" % (num, elapsed, num / elapsed)
