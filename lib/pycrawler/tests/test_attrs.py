
class A:
    a = 1
    b = 2

class B(A):
    a = 3
    b = 4

print dir(A())
print dir(B())
