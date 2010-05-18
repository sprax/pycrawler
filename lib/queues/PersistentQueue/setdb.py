#!/usr/bin/env python2.6

import sqlite3

class SetDB(object):
    """
    Implements a set database.

    One column, primary key, objects are there or not, with no additional columns.

    Useful for when such an operation needs to be fast.  While current implementation
    is slow, this is abstracted away to allow this component to be replaced when we
    need it.
    """

    def __init__(self, dbname):
        self.conn = sqlite3.connect(dbname)
        self.initialize()

    def initialize(self):
        try:
            with self.conn:
                self.conn.execute("CREATE TABLE keys (key TEXT PRIMARY KEY)")
        except sqlite3.OperationalError as exc:
            # If table already exists, ignore.
            pass

    def test_and_set(self, key):
        """
        If key is already in database, return True.
        Otherwise, add key to database, and return False.
        Operation is atomic, to ensure that this method may only
        return False once for a given key.
        """

        try:
            with self.conn:
                self.conn.execute("INSERT INTO keys (key) VALUES (?)", (key,))
            return False
        except sqlite3.IntegrityError as exc:
            return True

    def __contains__(self, key):
        c = self.conn.cursor()
        c.execute("SELECT 1 FROM keys WHERE key = ? LIMIT 1", (key,))
        return False if c.fetchone() is None else True
