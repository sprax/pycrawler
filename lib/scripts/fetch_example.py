"""
An example of using PyCrawler to fetch a list of URLs
"""
#$Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import sys
sys.path.extend(".")
import traceback
import multiprocessing
from time import time, sleep
from syslog import syslog, openlog, setlogmask, LOG_UPTO, LOG_INFO, LOG_DEBUG, LOG_NOTICE, LOG_NDELAY, LOG_CONS, LOG_PID, LOG_LOCAL0
from optparse import OptionParser
from PyCrawler import Fetcher, AnalyzerChain, GetLinks, SpeedDiagnostics, LogInfo

def main(options, args):
    print "making an AnalyzerChain"
    ac = AnalyzerChain()

    print "adding Analyzers"
    ac.append(GetLinks, 10)
    ac.append(SpeedDiagnostics, 1)

    # Prepare an instance of Fetcher, which can be anything that
    # implements the attrs and methods of Fetcher.Fetcher.
    fetcher = Fetcher(
        DOWNLOAD_TIMEOUT = options.download_timeout, 
        FETCHER_TIMEOUT  = options.fetcher_timeout,
        NUM_FETCHERS     = options.num_fetchers,
        outQ = ac.inQ,
        params = {"CRAWLER_NAME":      options.name,
                  "CRAWLER_HOMEPAGE":  options.homepage },
        )

    # get URLs
    if options.input is None:
        try:
            print "opening %s" % args[0]
            urls = open(args[0])
        except Exception, exc:            
            sys.exit("Use --input, or specify a file of URLs. (error: %s)" % exc)
        for c in range(options.max):
            u = urls.readline()
            if not u: break
            try:        
                fetcher.packer.add_url(u.strip())
            except Exception, e:
                print "fetcher.packer.add_url(%s) --> %s" % (u, e)
    else:
        try:
            errors = fetcher.packer.expand_from_file(options.input)
        except Exception, exc:
            sys.exit("Failed to load gziped json dump of previous Fetcher run:\n\n%s" % exc)

    print "fetcher has %d relurls" % len(fetcher.packer)

    if options.daemonize:
        try:
            import daemon
            print "Entering DaemonContext.  Logging to syslog."
            #with daemon.DaemonContext():
            #    wait_for_finish(ac, fetcher, quiet=options.quiet)
        except Exception, exc:
            print "Unable to daemonize.  Apparently python-daemon is not installed?"
            sys.exit(traceback.format_exc(exc))
    else:
        wait_for_finish(ac, fetcher, quiet=options.quiet)

def wait_for_finish(ac, fetcher, quiet=False):
    print "Logging to syslog."

    openlog("FetcherTests", LOG_NDELAY|LOG_CONS|LOG_PID, LOG_LOCAL0)
    if quiet:
        setlogmask(LOG_UPTO(LOG_INFO))

    from signal import signal, SIGHUP, SIGINT, SIGQUIT, SIGABRT, SIGTERM
    for sig in (SIGHUP, SIGINT, SIGQUIT, SIGABRT, SIGTERM):
        signal(sig, lambda a,b: fetcher.go.clear())

    syslog(LOG_DEBUG, "calling AnalyzerChain.start()")
    ac.start()
    syslog(LOG_DEBUG, "AnalyzerChain started")
    
    syslog(LOG_DEBUG, "calling Fetcher.start()")
    fetcher.start()
    syslog(LOG_DEBUG, "Fetcher started")

    syslog(LOG_DEBUG, "Entering while loop to wait for fetcher to perish.")
    while fetcher.is_alive():
        syslog(LOG_DEBUG, "Waiting for fetcher.  Children: %s" % multiprocessing.active_children())
        sleep(1)

    syslog(LOG_DEBUG, "fetcher perished, stopping")
    fetcher.stop()  # do we need to call this?

    syslog(LOG_DEBUG, "stopping the AnalyzerChain")
    ac.stop()

    syslog(LOG_DEBUG, "Waiting for any unfinished children.")
    while len(multiprocessing.active_children()) > 1:
        syslog(LOG_DEBUG, "Waiting for: %s" % multiprocessing.active_children())
        sleep(1)

    syslog(LOG_DEBUG, "Done.")

if __name__ == "__main__":
    parser = OptionParser(description=__doc__)
    parser.add_option("--num",   type=int, dest="num_fetchers",     default=1,    help="Number of Fetcher objects to run (only applies to MultiFetcher.")
    parser.add_option("--max",   type=int, dest="max",              default=10,   help="max number of URLs to fetch, limits lines read from file provided as an argument.  Does not limit data loaded by --input.")
    parser.add_option("--ltime", type=int, dest="fetcher_timeout",  default=100,  help="seconds to run the fetcher")
    parser.add_option("--dtime", type=int, dest="download_timeout", default=100,  help="seconds to allow for each fetch")
    parser.add_option("--output",          dest="output",           default="",   help="File path for storing discovered links.")
    parser.add_option("--input",           dest="input",            default=None, help="File path for loading a packed list of URLs.")
    parser.add_option("--homepage",        dest="homepage",         default=None, help="Homepage of your crawler, for user-agent string.")
    parser.add_option("--name",            dest="name",             default="PyCrawler", help="Name of your crawler, for user-agent and robots.txt checking.")
    parser.add_option("--daemonize",       dest="daemonize", action="store_true", default=False, help="Cause the fetcher to separate from your shell and run in the background.")
    parser.add_option("--quiet",           dest="quiet",     action="store_true", default=False, help="Print only DEBUG_INFO and higher priority information.")
    (options, args)= parser.parse_args()

    main(options, args)

