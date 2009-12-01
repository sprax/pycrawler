"""
An example of using PyCrawler.URL to process a file containing a list
of URLs
"""
#$Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import sys
import PyCrawler
from optparse import OptionParser

parser = OptionParser()
parser.add_option("--dump",     action="store_true", dest="dump",     default=False,  help="Pretty print a gzip'ed simplejson file of a URL.packer.dump()")
parser.add_option("--urls-only",action="store_true", dest="urls_only",default=False,  help="Dump only the absolute URLs of the packer to the file specified by --output")
parser.add_option("--summary",  action="store_true", dest="summary",  default=False,  help="Summarize the gzip'ed simplejson file of a URL.packer.dump()")
parser.add_option("--merge",    action="store_true", dest="merge",    default=False,  help="Load all filenames specified by args, checks first if each file is a gzip'ed packer dumps and then checks for one-URL-per-line, and merges them together in a single packer.  Dumps the merged packer to a gzip'ed JSON file that can be loaded by other components.")
parser.add_option("--output",                        dest="output_file", default="merged.json",  help="Output file path.")
(options, args)= parser.parse_args()

if options.merge:
    if len(args) == 0:
        sys.exit("Must specify one or more input files containing one URL per line.")
    main_packer = PyCrawler.URL.packer()
    for input_path in args:
        errors = main_packer.merge(input_path)
        if errors:
            print "Got %d errors for %s" % (len(errors), input_path)
            print "\t" + "\n\t".join(errors)
    # create the output file
    output_path = main_packer.dump_to_file(options.output_file)
    print "Created: %s" % output_path
    sys.exit()

# assume the first arg is a gzip'ed JSON file path
if not args:
    sys.exit("Requires a gzipped json file as an input arg.")

main_packer = PyCrawler.URL.packer()
try:
    main_packer.expand_from_file(args[0])
except Exception, exc:
    sys.exit("Failed to expand_from_file(%s), because: %s" % (args[0], exc))

if options.urls_only:
    output_path = main_packer.dump_absolute_urls(options.output_file)
    print "Created: %s" % output_path
    sys.exit()

if options.dump:        
    print main_packer.pformat()

if options.summary:
    stats = {}
    for hostkey, relurls in main_packer.dump():
        for relurl, depth, last_modified, http_response, content_data in relurls:
            if depth not in stats:
                stats[depth] = {"fetched": 0,
                                "unattempted": 0,
                                "failed": 0,
                                "total": 0,
                                "hosts": {}
                                }
            stats[depth]["hosts"][hostkey] = True                    
            stats[depth]["total"] += 1
            if last_modified == -1:
                stats[depth]["failed"] += 1
            elif last_modified == 0:
                stats[depth]["unattempted"] += 1
            else:
                stats[depth]["fetched"] += 1

    import locale
    print locale.setlocale(locale.LC_ALL, ("en", "utf-8"))
    def num(n, size = 13):
        s = locale.format('%d', n, True)
        return "%s%s" % (s, " " * (size - len(s)))
    
    depths = stats.keys()
    depths.sort()
    print """
        depth      %s
num hosts          %s
num total relurls  %s
num unattempted    %s
num fetched        %s
num failed         %s
""" % ("\t".join([num(depth)                        for depth in depths]),
       "\t".join([num(len(stats[depth]["hosts"])) for depth in depths]),
       "\t".join([num(stats[depth]["total"])        for depth in depths]),
       "\t".join([num(stats[depth]["unattempted"])  for depth in depths]),
       "\t".join([num(stats[depth]["fetched"])      for depth in depths]),
       "\t".join([num(stats[depth]["failed"])       for depth in depths]),
       )
