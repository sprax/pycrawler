import os
from time import sleep
import multiprocessing
import multiprocessing.managers
import syslog

class Process(multiprocessing.Process):

    # create our own subclass of SyncManager
    class Manager(multiprocessing.managers.SyncManager): pass

    def __init__(self):
        multiprocessing.Process.__init__(self)

        # instantiate the manager and get it started
        self.manager = self.Manager()
        self.manager.start()

        # make an Event for the while loop below
        self.go = self.manager.Event()
        self.go.set()
 
    def run(self):
        # we are now running inside a child process
        syslog.syslog("in run, pid: %d" % os.getpid())
        syslog.syslog("looking a go: " + str(dir(self.go)))
        syslog.syslog("looking a go: " + str(self.go.is_set))
        # "go" came from self.manager created in __init__w
        while self.go.is_set():
            sleep(1)
        syslog.syslog("exiting run")

    def stop(self):
        syslog.syslog("stopping, pid: %d" % os.getpid())
        self.go.clear()

if __name__ == '__main__':
    syslog.syslog("creating process, pid: %d" % os.getpid())
    p = Process()
    p.start()

    sleep(1)

    p.stop()
