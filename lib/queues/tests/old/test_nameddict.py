import sys

class InitFromSlots(type):
    def __new__(meta, name, bases, bodydict):
        slots = bodydict['__slots__']
        if slots and '__init__' not in bodydict:
            parts = ['def __init__(self, %s):' % ', '.join(slots)]
            for slot in slots:
                parts.append('    self.%s = %s' % (slot, slot))
            exec '\n'.join(parts) in bodydict
        super_new =  super(InitFromSlots, meta).__new__
        return super_new(meta, name, bases, bodydict)

class Record(object):
    __metaclass__ = InitFromSlots
    __slots__ = ()
    def _items(self):
        for name in self.__slots__:
            yield name, getattr(self, name)
    def __repr__(self):
        args = ', '.join('%s=%r' % tup for tup in self._items())
        return '%s(%s)' % (type(self).__name__, args)
    def __iter__(self):
        for name in self.__slots__:
            yield getattr(self, name)
    def __getstate__(self):
        return dict(self._items())
    def __setstate__(self, statedict):
        self.__init__(**statedict)

def make_record(typename, field_names):
    field_names = "'%s'" % "', '".join(field_names)
    template = """class %(typename)s(Record):
        __slots__ = %(field_names)s""" % locals()

    # using concepts from /usr/lib/python2.6/collections.py, so this
    # is Python Licensed:
    namespace = dict({"Record": Record})
    try:
        exec template in namespace
    except SyntaxError, e:
        raise SyntaxError(e.message + ":\n" + template)
    result = namespace[typename]

    # For pickling to work, the __module__ variable needs to be set to the frame
    # where the named tuple is created.  Bypass this step in enviroments where
    # sys._getframe is not defined (Jython for example).
    if hasattr(sys, '_getframe'):
        result.__module__ = sys._getframe(1).f_globals.get('__name__', '__main__')

    return result

if __name__ == '__main__':
    class Point(Record):
        __slots__ = 'x', 'y'
    
    p = Point(3, 4)
    p = Point(y=5, x=2)
    p = Point(-1, 42)
    assert (p.x, p.y) == (-1, 42), str((p.x, p.y))
    x, y = p
    assert (x, y) == (-1, 42), str((x, y))

    class Badger(Record):
        __slots__ = 'large', 'wooden'
    badger = Badger('spam', 'eggs')
    import cPickle as pickle
    assert repr(pickle.loads(pickle.dumps(badger))) == repr(badger)

    class Answer(Record):
        __slots__ = 'life', 'universe', 'everything'
    a1 = repr(Answer(42, 42, 42))
    a2 = eval(a1)
    assert repr(a2) == a1, str((repr(a2), a1))

    a2.life = 37
    ra2 = repr(a2)
    a3 = eval(ra2)
    assert repr(a3) == ra2, str((repr(a3), ra2))
    assert a3.life == 37

    Dog = make_record("Dog", ("legs", "name"))
    d = Dog(legs=4, name="barny")
    assert repr(d) == """Dog(legs=4, name='barny')""", repr(d)

    assert repr(pickle.loads(pickle.dumps(d))) == repr(d)
