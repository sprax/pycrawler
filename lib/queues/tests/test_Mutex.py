
import tempfile
from PersistentQueue import Mutex

class TestMutex():
    def acquire_callback(self, *args, **kwargs):
        assert not args and not kwargs
        self.acquired += 1

    def release_callback(self, *args, **kwargs):
        assert not args and not kwargs
        self.released += 1

    def test_simple(self):
        s = tempfile.NamedTemporaryFile()

        self.acquired = 0
        self.released = 0

        m = Mutex(lock_path = s.name,
                  acquire_callback = self.acquire_callback,
                  release_callback = self.release_callback)

        assert self.acquired == self.released == 0

        assert m.available() == True

        assert self.acquired == 0, "Acquired is %d, but should be 0" % self.acquired
        assert self.released == 0

        assert m.acquire() == True

        assert self.acquired == 1, "Acquired is %d, but should be 1" % self.acquired
        assert self.released == 0

        m.release()
        assert self.acquired == 1, "Acquired is %d, but should be 1" % self.acquired
        assert self.released == 1

        # Test multiple releases
        m.release()
        assert self.acquired == 1, "Acquired is %d, but should be 1" % self.acquired
        assert self.released == 1

    def test_multiple_locks(self):
        s = tempfile.NamedTemporaryFile()

        self.acquired = 0
        self.released = 0

        m1 = Mutex(lock_path = s.name,
                   acquire_callback = self.acquire_callback,
                   release_callback = self.release_callback)

        m2 = Mutex(lock_path = s.name,
                   acquire_callback = self.acquire_callback,
                   release_callback = self.release_callback)

        assert m1.available() == m2.available() == True

        assert m1.acquire() == True

        assert m2.available() == False

        assert m2.acquire(block=False) == False
        assert self.acquired == 1
        assert self.released == 0

        assert m1.available() == False
        assert m2.available() == False

        assert m1.release() is None
        assert self.acquired == 1
        assert self.released == 1

        assert m1.available() == True

    def test_fake_mutex(self):
        """ Test fake Mutex interface. """

        self.acquired = self.released = 0
        m = Mutex(None,
                  acquire_callback = self.acquire_callback,
                  release_callback = self.release_callback)

        assert m.available()
        assert m.acquire() == True

        # FIXME: fails.  fake mutex doesn't implement,
        # and thus provides an incompatible interface
        #assert m.acquire(block=False) == True

        assert m.available()
        assert m.release() is None

        assert self.acquired == self.released == 0
