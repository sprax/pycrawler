from collections import namedtuple
class Point(namedtuple('Point', 'x y')):
    __slots__ = ()
    @property
    def hypot(self):
        print "hi"
        return (self.x ** 2 + self.y ** 2) ** 0.5
    def __str__(self):
        return 'Point: x=%6.3f  y=%6.3f  hypot=%6.3f' % (self.x, self.y, self.hypot)

p = Point(4,5)
print p.hypot
print p.hypot
print p.hypot
print p.hypot
