"""
simple nameddict implementation that can be pickled

TODO: Perhaps there is a better way to do this using collections?
"""
#$Id$
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
import sys
class nameddict:
    """
    simple nameddict implementation that can be pickled
    """
    def __init__(self, attrs):
        self.__dict__.update(attrs)
        # For pickling to work, the __module__ variable needs to be
        # set to the frame where the named dict is created.  Bypass
        # this step in enviroments where sys._getframe is not defined
        # (Jython for example) or sys._getframe is not defined for
        # arguments greater than 0 (IronPython).
        try:
            self.__module__ = sys._getframe(1).f_globals.get('__name__', '__main__')
        except (AttributeError, ValueError):
            pass

    def __getinitargs__(self):
        return (self.__dict__,)

if __name__ == '__main__':
    # verify that instances can be pickled
    from cPickle import loads, dumps
    d  = nameddict({'a': 1.0, 'b': None})
    p  = dumps(d, -1)
    pd = loads(p)
    print d.__dict__
    print pd.__dict__
    assert d == pd
    print "... but it works anyway... "
