import os
from time import sleep
import multiprocessing
import multiprocessing.managers
from syslog import syslog

class Process(multiprocessing.Process):
    # create our own subclass of SyncManager
    class Manager(multiprocessing.managers.SyncManager): pass

    def __init__(self):
        multiprocessing.Process.__init__(self)

        self.go = multiprocessing.Event()
        self.go.set()
 
    def run(self):
        # instantiate the manager and get it started
        self.manager = self.Manager()
        self.manager.start()

        while self.go.is_set():
            try:
                syslog("Got: %s" % str(self.inQ.get_nowait()))
            except Queue.Empty:
                sleep(1)
                continue
            except Exception, exc:
                syslog("Hit problem: %s" % traceback.format_exc(exc))
        syslog("exiting run")

    def stop(self):
        syslog("stopping, pid: %d" % os.getpid())
        self.go.clear()

if __name__ == '__main__':
    syslog("creating process, pid: %d" % os.getpid())
    p = Process()
    p.start()

    sleep(1)

    p.stop()
