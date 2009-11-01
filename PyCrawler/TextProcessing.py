"""
A simple set of functions for processing text.
"""
#$Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import re
import URL

whitespace_re = re.compile("\s+", re.M)
def is_text(bytes):
    """
    Tests if input bytes are text using the same algorithm as used in
    perl.  Returns a two-tuple:

       (reason string, boolean)

    """
    if not bytes:
        return ("empty string", True)
    # only decides on the first kb regardless regardless
    bytes = bytes[:1024]
    if bytes.find("\0") > -1:
        return ("is not texty because found \\0", False)
    startlen = len(bytes)
    # strip out all printable chars
    bytes = re.sub("[\040-\176]+", "", bytes)
    # strip out all whitespace too
    bytes = whitespace_re.sub("", bytes)
    # 30% threshhold
    if len(bytes) > 0.3 * startlen:
        return ("too much is not text", False)
    return ("", True)

anchors_re = re.compile(r"<a\s+([^>]*)>(.*?)</a>")
href_re = re.compile(r"""href\s*=(\s*'([^']*)'|\s*"([^"]*)"|(\S+))""", re.I)
scheme_re = re.compile(r"^(\w+)\://(.*)$")
js_re = re.compile(r"\s*javascript\:", re.I)
def get_links(hostkey, relurl, text, depth=0, accepted_schemes=None):
    """
    Uses python regexes to extract links from text and uses URL.packer
    to construct a list of (host, [relurls]) tuples.  Also returns a
    list of errors encountered.  Usage:

    errors, host_and_relurls_list = get_links(hostkey, relurl, text, ACCEPTED_SCHEMES)

    Issues:
      * if 'relurl' is a dir, then it must end in '/'
    """
    errors = []
    (reason, is_texty) = is_text(text)
    if not is_texty:
        errors.append(reason)
        return errors, []
    local_packer = URL.packer()
    relurl = relurl.strip()  # prevent newline from appearing in constructed links
    path = relurl.split('/')[:-1] # if a dir, it must end in '/'
    try:
        anchors = anchors_re.findall(text)
    except Exception, exc:
        errors.append("anchors_re.findall(text) --> %s" % str(exc))
        return errors, []
    if not anchors:
        return errors, []
    for attrs, anchor in anchors:
        href_m = href_re.search(attrs)
        if not href_m:
            errors.append("found no href attr in %s" % repr(attrs))
            continue
        url = href_m.group(2) or href_m.group(3) or href_m.group(4)
        # do not accept URLs that have funky chars
        try:
            assert(url == url.encode("utf-8", "delete"))
        except:
            errors.append("bad utf-8 in %s" % repr(url))
            continue
        # drop javascript: now rather than trying to parse a URL and
        # reject the scheme.
        if js_re.match(url):  
            continue
        # suck out all whitespace that might have existed in the url,
        # this usually means it is busted, but try anyway
        url = whitespace_re.sub("", str(url))
        scheme_m = scheme_re.match(url)
        if not scheme_m:
            # assume is relurl --> construct absolute URL
            new_path = []
            if url[0] == "/":
                path_parts = url.split('/')
            else:
                path_parts = path + url.split('/')
            path_error = False
            for step in path_parts:
                if step == "..":
                    try:
                        new_path.pop()
                    except IndexError:
                        errors.append("bad step count, parse error? %s" % repr(url))
                        path_error = True
                        break
                elif step in [".", ""]:
                    continue
                else:
                    new_path.append(step)
            if path_error: continue
            relurl = '/' + '/'.join(new_path)
            if url[-1] == "/": # is a dir
                relurl += '/'
            url = hostkey + relurl
        else:
            scheme = scheme_m.group(1)
            if accepted_schemes and \
                    scheme not in accepted_schemes:
                errors.append("rejecting scheme: %s" % repr(scheme))
                continue
        try:
            local_packer.add_url(url, depth = depth - 1)
        except Exception, e:
            continue
    return errors, local_packer.dump()

def tests():
    "tests for get_links"
    fake_doc = """
<a href="/foo1">1</a>
<a href="../foo2">2</a>
<a href="./foo3">3</a>
<a href="path1//foo4">4</a>
<a href="path2/..//path3/path4/../../path5//foo5/">4</a>
"""
    host = "https://crazyhost.com"
    errors, packer_dump = get_links(host, "/dog/", fake_doc)
    pprint.pprint(errors)
    pprint.pprint(packer_dump)
    rec = packer_dump[0]
    assert host == rec[0]
    relurls = [x[0] for x in rec[1]]
    expected_relurls = [
        "/foo1",
        "/foo2",
        "/dog/foo3",
        "/dog/path1/foo4",
        "/dog/path5/foo5/",
        ]
    wrong = False
    for expected in expected_relurls:
        if expected not in relurls:
            print "Failed to find %s" % expected
            wrong = True
    if wrong:
        raise Exception("Test failed for reasons above")
    else:
        print "Test passed"

if __name__ == "__main__":
    import sys
    import pprint
    from optparse import OptionParser
    parser = OptionParser(usage="",
                           description=__doc__)
    parser.add_option("--host",     dest="host",   default="",    help="")
    parser.add_option("--relurl",   dest="relurl", default="",    help="")
    parser.add_option("--file",     dest="file",   default="",    help="")
    parser.add_option("--test",     dest="test",   default=False, action="store_true",  help="Run tests for get_links")
    (options, args)= parser.parse_args()
    if options.test:
        tests()
        sys.exit(0)

    errors, new_packer = get_links(options.host, options.relurl, open(options.file).read())
    if errors:
        print "Got errors:"
        print pprint.pprint(errors)
    print new_packer.pformat()
