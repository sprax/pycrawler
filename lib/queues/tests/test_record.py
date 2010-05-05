#!/usr/bin/python2.6
"""
tests for PersistentQueue/record.py
"""
# $Id$
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
__maintainer__ = "John R. Frank"

import cPickle as pickle

from PersistentQueue import define_record

from nose.tools import assert_raises, raises

class TestRecordConstructor(object):
    Point2D = define_record('Point2D', ['x', 'y'])
    def __init__(self):
        self.cls = self.Point2D

    def test_module_name(self):
        assert self.Point2D.__module__ == __name__

    @raises(TypeError)
    def test_invalid_slot(self):
        self.cls(x=1, z=2)

    @raises(TypeError)
    def test_no_x(self):
        self.cls(y=2)

    @raises(TypeError)
    def test_no_y(self):
        self.cls(x=1)

    @raises(TypeError)
    def test_no_args(self):
        self.cls()

    def test_no_kwargs(self):
        a = self.cls(1, 2)
        assert a.x == 1 and a.y == 2

class TestRecordOperations(object):
    Point2D = define_record('Point2D', ['x', 'y'])

    def __init__(self):
        self.cls = self.Point2D
        self.a = self.cls(x=1, y=2)
        self.b = self.cls(x=100, y=200)

    def test_repr(self):
        assert repr(self.a) == 'Point2D(x=1, y=2)'

    def test_str(self):
        assert str(self.a) == repr(self.a)

    def test_iter(self):
        assert list(iter(self.a)) == [self.a.x, self.a.y]

    def test_equality(self):
        c = self.cls(x=self.a.x, y=self.a.y)
        assert (self.a == c) == True
        assert (self.a == self.b) == False
        assert (self.a != c) == False
        assert (self.a != self.b) == True

    def test_index(self):
        assert_raises(IndexError, lambda: self.a[2])
        assert_raises(IndexError, lambda: self.a[-3])

        assert self.a[0] == self.a.x == 1 == self.a[-2]
        assert self.a[1] == self.a.y == 2 == self.a[-1]

    def test_len(self):
        assert len(self.a) == 2
        assert len(self.b) == 2

    @raises(pickle.PicklingError)
    def test_pickle_notglobal(self):
        """ Test that if the class doesn't share a global name,
        picklig fails."""

        assert not hasattr(__name__, self.a.__class__.__name__)

        s = pickle.dumps(self.a)
        pickle.loads(s)

    def test_pickle(self):
        assert not hasattr(__name__, 'Point2D')

        global Point2D
        Point2D = self.cls
        try:
            s = pickle.dumps(self.a)
            c = pickle.loads(s)
            assert self.a == c
        finally:
            del Point2D

class TestDefineRecord(object):
    def test_class_names(self):
        assert_raises(ValueError, define_record, 'Point3D-', [])
        assert_raises(ValueError, define_record, '3DPoint', [])
        assert_raises(ValueError, define_record, 'class', [])
        
    def test_field_names(self):
        assert_raises(ValueError, define_record, 'Point3D', ['x', 'y', '1z'])
        assert_raises(ValueError, define_record, 'Point3D', ['x', 'y', 'def'])
        assert_raises(ValueError, define_record, 'Point3D', ['x', 'y', 'x'])
        assert_raises(ValueError, define_record, 'Point3D', ['_x', 'y', 'z'])
