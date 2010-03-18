#!/usr/bin/python2.6
"""
tests for PersistentQueue/record_factory.py
"""
# $Id: test_records.py 115 2010-01-18 16:34:51Z postshift@gmail.com $
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
__maintainer__ = "John R. Frank"
import os
import sys
import copy
import cPickle as pickle
import StringIO

from PersistentQueue import define_record, RecordFactory, Static

from nose.tools import assert_raises

class TestRecordFactoryConstructor(object):
    def setUp(self):
        self.cls = define_record('Point2D', ['x', 'y'])

    def test_template_length(self):
        """
        Assert that RecordFactory raises exception when we give it wrong number
        of template type arguments.
        """
        assert_raises(AssertionError, RecordFactory, self.cls, [])
        assert_raises(AssertionError, RecordFactory, self.cls, [str])
        assert isinstance(RecordFactory(self.cls, [float, float]), RecordFactory)

class TestRecordFactorySimple(object):
    def setUp(self):
        self.cls = define_record('Point2D', ['x', 'y'])

    def test_static(self):
        """ Test that dumps and loads for Static types works. """
        factory = RecordFactory(self.cls, [Static('first'), Static('second')])

        rec = factory.create(1, 2)
        recval = self.cls(x='first', y='second')
        assert rec == recval
        
        s = factory.dumps(rec)
        assert s == '|'

        rec2 = factory.loads(s)
        assert rec2 == recval

    def test_none(self):
        """ Test that values of NoneType work correctly. """
        factory = RecordFactory(self.cls, [int, int])

        rec = factory.create(1, None)
        assert rec == self.cls(x=1, y=None)

        s = factory.dumps(rec)
        assert s == '1|'

        rec2 = factory.loads(s)
        assert rec == rec2

    def test_bool(self):
        """ Test that values of type bool work correctly. """
        factory = RecordFactory(self.cls, [bool, bool])

        rec = factory.create(1311, 0)
        #assert rec == self.cls(x=True, y=False)

        s = factory.dumps(rec)
        assert s == '1311|0'

        rec2 = factory.loads(s)
        assert rec2 == self.cls(x=True, y=False)

        # FIXME: bug?
        assert rec != rec2

    def test_stringval(self):
        factory = RecordFactory(self.cls, [unicode, unicode])

        rec = factory.create('x', 'y')
        assert rec == self.cls(x='x', y='y')

        s = factory.dumps(rec)
        assert s == 'x|y'

        rec2 = factory.loads(s)
        assert rec2 == rec

    def test_loads(self):
        factory = RecordFactory(self.cls, [unicode, unicode])
        assert_raises(factory.BadFormat, factory.loads, 7)
        assert_raises(factory.BadFormat, factory.loads, StringIO.StringIO('x|y'))
        assert_raises(factory.BadFormat, factory.loads, 'x')
        assert_raises(factory.BadFormat, factory.loads, 'x|y|z')
