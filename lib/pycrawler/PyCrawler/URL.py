#!/usr/bin/python2.6
"""
URL.packer provides a method called add_url that parses URLs and
appends them to lists in a dictionary.  The keys of the dictionary are
the hostkey for the URL.  A hostkey is scheme://hostname:port

While parsing URLs, the add_url method might raise BadFormat or
UnsupportedScheme exceptions.
"""
# $Id$
__author__ = "John R. Frank"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"

import os
import re
import gzip
import pprint
import traceback
import simplejson
from hashlib import md5
from urlparse import urlparse, urlunparse
ACCEPTED_SCHEMES = ("http", "https", "ftp", "ftps")
"""
TODO:
 
 * fix command line options to make more sense and expose them as a
   script rather than part of this module

 * figure out if we can use python's newer version of urlparse to do
   more of this

>>> from urlparse import urlparse
>>> urlparse("http://dogs.com/cars;type=a?bob=dog#eat")
('http', 'dogs.com', '/cars', 'type=a', 'bob=dog', 'eat')
        Parse a URL into 6 components:
        <scheme>://<netloc>/<path>;<params>?<query>#<fragment>
        Return a 6-tuple: (scheme, netloc, path, params, query, fragment).
"""
class URLException(Exception):
    """General URL Exception"""
    def __init__(self, url):
        """puts the URL in a known attr"""
        self.url = url

class BadFormat(URLException): 
    """Raised when the urlparse library fails to parse a URL"""
    def __str__(self):
        return "%s --> %s" % (self.url, traceback.format_exc(self))

def get_hostbin(hostid):
    "Returns the hostbin for a hostid"
    return "/".join([hostid[:2], hostid[2:4], hostid[4:6]])

def get_hostbin_id(hostname):
    """
    Returns the hostid for the hostkey, which is just the hexdigest of
    the md5 sum, and also the hostbin, which is the three directory
    tiers built from the hostid.
    """
    hostid = md5(hostname).hexdigest()
    hostbin = get_hostbin(hostid)
    return hostbin, hostid

def get_hostid_docid(scheme, hostname, port, relurl):
    """
    Generates md5 for the hostname and for the fullurl
    """
    return md5(hostname).hexdigest(), \
        md5(fullurl(scheme, hostname, port, relurl)).hexdigest()

def get_parts(url):
    """
    Cleanly splits an absolute URL into four strings: "scheme"
    (without ://), hostname, ":port", and relurl:

         scheme://hostname:port  -- which we call "hostkey"
         ^^^^^^           ^^^^^

         /relative/path/to/file -- which we call "relurl"

    The colon symbol is included in the port string, if present.

    Also raises appropriate errors as needed.
    """
    try:
        o = urlparse(url)
    except Exception, e:
        raise BadFormat(url)
    scheme = o.scheme.lower()
    hostname = o.hostname.lower()
    try:
        if o.port:
            port = ":%d" % o.port # int, if parsed without error
        else:
            port = ""
    except:
        raise BadFormat(url)
    # reconstruct relurl without scheme, netloc, nor fragment
    relurl = urlunparse(("", "", o.path, o.params, o.query, ""))
    if relurl == "":
        relurl = "/"
    return scheme, hostname, port, relurl

def fullurl(scheme, hostname=None, port=None, relurl=None):
    if hasattr(scheme, "hostname"):
        rec = scheme
        return "".join((rec.scheme, "://", rec.hostname, rec.port, rec.relurl))
    else:
        return "".join((scheme, "://", hostname, port, relurl))

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
def get_links(hostkey, relurl, text, depth=0, accepted_schemes=ACCEPTED_SCHEMES):
    """
    Uses python regexes to extract links from text and uses URL.packer
    to construct a list of (host, [relurls]) tuples.  Also returns a
    list of errors encountered.  Usage:

    errors, host_and_relurls_list = get_links(hostkey, relurl, text, ACCEPTED_SCHEMES)

    Issues:
      * if 'relurl' is a dir, then it must end in '/'
    """
    links = []
    errors = []
    (reason, is_texty) = is_text(text)
    if not is_texty:
        errors.append(reason)
        return errors, links
    relurl = relurl.strip()  # prevent newline from appearing in constructed links
    path = relurl.split('/')[:-1] # if a dir, it must end in '/'
    try:
        anchors = anchors_re.findall(text)
    except Exception, exc:
        errors.append("anchors_re.findall(text) --> %s" % str(exc))
        return errors, links
    if not anchors:
        return errors, links
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
            parts = get_parts(url)
        except BadFormat, exc:
            errors.append(str(exc))
        links.append(parts)
    # return de-duplicated links
    return errors, list(set(links))
