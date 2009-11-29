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
import gzip
import pprint
import traceback
import simplejson
from hashlib import md5
from urlparse import urlparse, urlunparse
"""
TODO:
 
 * get ACCEPTED_SCHEMES from some place better

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

def get_hostkey_relurl(url, schemes=("http", "https")):
    """
    Cleanly splits an absolute URL into two strings:

         scheme://hostname  -- which we call "hostkey"

         /relative/path/to/file -- which we call "relurl"

    Also raises appropriate errors as needed.
    """
    try:
        o = urlparse(url)
    except Exception, e:
        raise BadFormat(url)
    scheme = o.scheme.lower()
    if scheme not in schemes:
        raise UnsupportedScheme(url)
    hostkey = "%s://%s" % (scheme, o.hostname)
    if o.port:
        hostkey += ":%d" % o.port
    # reconstruct relurl without scheme, netloc, nor fragment
    relurl = urlunparse(("", "", o.path, o.params, o.query, ""))
    if relurl == "":
        relurl = "/"
    return hostkey, relurl

class URLException(Exception):
    """General URL Exception"""
    def __init__(self, url):
        """puts the URL in a known attr"""
        self.url = url

class BadFormat(URLException): 
    """Raised when the urlparse library fails to parse a URL"""
    def __str__(self):
        return "%s --> %s" % (self.url, traceback.format_exc(self))

class UnsupportedScheme(URLException):
    """Raised when this URL has a scheme not in the accepted list"""
    def __str__(self):
        return ("Unsupported scheme: %s" % self.url)

class NotAcceptedByRegex(URLException):
    """Raised when a URL fails the accepted regex test"""
    def __str__(self):
        return ("Not accepted by regex %s" % self.url)

class RejectedByRegex(URLException): 
    """Raised when a URL fails the rejected regex test"""
    def __str__(self):
        return ("Rejected by regex %s" % self.url)

class packer:
    """
    A utility for construct a list of hosts and for each host a list
    of relative urls with link depth and last-modified times for each
    relurl.
    """
    def __init__(self, schemes=("http", "https")):
        self.hosts = {}
        self.schemes = schemes
        self.accepts = None
        self.rejects = None

    def __len__(self):
        tot = 0
        for k in self.hosts:
            tot += len(self.hosts[k])
        return tot

    def set_global_regexes(self, accept=None, reject= None):
        self.accept = accept
        self.reject = reject

    def add_url(
        self, url, depth=0, last_modified=0, 
        http_response=None, content_data=None):
        """
        Add a URL to the appropriate host's list of relative URLs.
        depth and last_modified have default values of 0 and 0.
        """
        hostkey, relurl = get_hostkey_relurl(url, schemes=self.schemes)
        self.update(
            hostkey, relurl, 
            depth, last_modified, http_response, 
            content_data,
           )

    def update(
        self, hostkey, relurl, depth, 
        last_modified, http_response, content_data):
        """
        An internal method for keeping consistent and unique
        information.  Use add_url or expand methods instead.
        """
        # make sure that hostkeys and relurls are not unicode
        relurl  = str(relurl)
        hostkey = str(hostkey)
        # apply global regexes
        if self.accept and not self.accept.match(relurl):
            raise NotAcceptedByRegex(relurl)
        if self.reject and self.reject.match(relurl):
            raise RejectedByRegex(relurl)
        if hostkey not in self.hosts:
            self.hosts[hostkey] = {}
        if relurl in self.hosts[hostkey]:
            (old_depth, old_last_modified, old_http_response, old_content_data) = \
                self.hosts[hostkey][relurl]
            # keep the most shallow
            if old_depth < depth:
                depth = old_depth
            # keep the most recent
            if old_last_modified > last_modified:
                last_modified = old_last_modified
            # drop old_http_response and old_content_data and replace
            # with the new stuff
            if http_response is None: http_response = old_http_response
            if content_data is None: content_data = old_content_data
        self.hosts[ hostkey ][ relurl ] = (depth, last_modified, http_response, content_data)

    def dump(self):
        """
        Dumps a list of two-tuples.  Each tuple has:

            (hostkey, [ [ relurl, depth, last_modified, http_response, content_data ] ])

        """
        out = []
        for hostkey in self.hosts:
            recs = []
            for relurl in self.hosts[hostkey]:
                (depth, last_modified, http_response, content_data) = \
                    self.hosts[hostkey][relurl]
                recs.append(
                    (relurl, 
                      depth, last_modified, http_response,
                      content_data,
                     ) 
                   )
            out.append((hostkey, recs))
        return out

    def dump_to_file(self, output_path, gz=True, make_file_name_unique=False):
        """
        Serializes the results of self.dump() using simplejson and
        puts the data into a file at output_path.

        If gz is True (the default), this appends .gz to output_path
        and writes gzipped data to the file.
        """
        parent = os.path.dirname(output_path)
        if parent and not os.path.exists(parent):
            os.makedirs(parent)
        json = simplejson.dumps(self.dump())
        if make_file_name_unique:
            output_path += "." + md5(json).hexdigest()
        if gz:
            output_path += ".gz"
            zbuf = open(output_path, "w")
            file = gzip.GzipFile(mode = "wb",  fileobj = zbuf, compresslevel = 9)
        else:
            file = open(output_path, "w")
        file.write(json)
        file.close()
        return output_path
        
    def pformat(self):
        """
        Returns a pretty formatted string of the packer.
        """
        return pprint.pformat(self.dump())

    def expand_from_file(self, input_path, gz=True):
        """
        Expands the packer using data deserialized from the file at
        input_path using simplejson.

        If gz is True (the default), this checks for input_path and
        also input_path.gz
        """
        if os.access(input_path, os.R_OK):
            zbuf = open(input_path, "r")
        elif gz:
            zbuf = open(input_path + ".gz", "r")
        zfile = gzip.GzipFile(mode = "r",  fileobj = zbuf, compresslevel = 9)
        json = zfile.read()
        zfile.close()
        zbuf.close()
        data = simplejson.loads(json)
        return self.expand(data)

    def expand(self, host_and_relurls_list):
        """
        Expand relurl lists using output of another URL.packer.dump()
        """
        errors = []
        for (hostkey, relurls) in host_and_relurls_list:
            for (relurl, depth, last_modified, http_response, content_data) in relurls:
                try:
                    self.update(hostkey, relurl, depth, last_modified, http_response, content_data)
                except Exception, e:
                    errors.append(str(e))
        return errors

    def merge(self, input_path):
        """
        Try first to expand_from_file(input_path) assuming gz=True,
        and if that fails, then try to add one-URL-per-line

        Returns a list of errors encountered.
        """
        errors = []
        try:
            self.expand_from_file(input_path)
        except Exception, exc:
            errors.append(str(exc))
            try:
                fh = open(input_path)
            except Exception, exc:
                errors.append("Skipping %s because %s" % (
                        input_path,
                        traceback.format_exc(exc),
                       ))
                return errors
            for u in fh.readlines():
                try:
                    self.add_url(u.strip())
                except Exception, exc:
                    errors.append("skipping %s because %s" % (
                            repr(u),
                            traceback.format_exc(exc),
                           ))
            fh.close()
        return errors

