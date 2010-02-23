
from collections import namedtuple
import sys
import StringIO

class Point(namedtuple('Point', 'x y')):
    __slots__ = ()
    @property
    def hypot(self):
        print "hi"
        return (self.x ** 2 + self.y ** 2) ** 0.5
    def __str__(self):
        return 'Point: x=%6.3f  y=%6.3f  hypot=%6.3f' % (self.x, self.y, self.hypot)

def test_namedtuple():
    p = Point(4,5)
    oldstdout = sys.stdout
    try:
        sys.stdout = StringIO.StringIO()

        assert str(p.hypot) == str(p.hypot) == str(p.hypot)
        assert sys.stdout.getvalue() == 'hi\nhi\nhi\n'

        sys.stdout = StringIO.StringIO()
        assert str(p.hypot) == str(41 ** 0.5)
        assert sys.stdout.getvalue() == 'hi\n'
    finally:
        sys.stdout = oldstdout
