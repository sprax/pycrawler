#==== ==== ==== ==== ==== ==== ==== ====
#This should get rss link from a page
#1. It trys it simple by the standard rss tag (gets multiple)
#2. If no standard are found: it searches for links that might be rss
#3. It checks if they are rss, if not it opens the page and search there on - recursively
#==== ==== ==== ==== ==== ==== ==== ====
import codecs
import re
import urllib
from BeautifulSoup import BeautifulSoup
from xml.dom import minidom
from prettySoup import pretty

#==== ==== ==== ====
#HTML parameters to dictionary
#==== ==== ==== ====
def parametersToDict(s):
    return dict([i.split("=") for i in s.replace("\"", "").split()])

#pageList = [ u'http://www.reuters.com']

#==== ==== ==== ====
#CHECK if the link is a RSS valid xml
#==== ==== ==== ====
def isRss(link):
    try:
        v = minidom.parse(urllib.urlopen(link))
        if v.getElementsByTagName('rss'):
            return True
        return False
    except Exception, inst:
        #print "\n".join([str(type(inst)), str(inst.args), str(inst)])
        return False

#==== ==== ==== ====
#If its only a relativ link change it to absolute
#==== ==== ==== ====
def toHttp(href, page):
    return [page, ""][href.startswith("http")] + href

#==== ==== ==== ====
#Root http adress
#==== ==== ==== ====
def rootHttpAdress(link):
    return "//".join(link.split("/")[0:3:2])

#==== ==== ==== ====
#FIND RSS ON A PAGE - RECURSIVELY
#==== ==== ==== ====
def goGetRss(page, depth=3, searched=0):
    if depth == 0:
        return []
    g = []
    #==== 1. START ====
    #print "\n==== ==== ==== ===="
    #print "page:", page
    #==== 2. OPEN IN SOUP ====
    try:
        p = urllib.urlopen(page).read()
    except:
        p = ""
    print "searched:", searched
    p = BeautifulSoup(pretty(p))
    #==== 3. FIND STANDARD RSS ====
    rssxml = p.findAll('link', {'rel': 'alternate', 'type': 'application/rss+xml'})
    if rssxml:
        print rssxml, #rssxml["title"]#, "rssLink:", rssLink, "IsRss:", isRss(rssLink)
        g += [toHttp(i["href"], page) for i in rssxml]
    #else:
    #print "==== No rss found ===="
    #==== 4. FIND NON-STANDARD RSS OR PAGES THAT MIGHT HAVE LIST OF RSS ====
    otherRssLike = p.findAll('a', attrs={'href' : re.compile("rss|feed")})
    otherRssLike = set([toHttp(a['href'], page) for a in otherRssLike]) #to long http - set
    #==== 5. VALID RSS ====
    otherRss = [a for a in otherRssLike if isRss(a)]
    #==== 6. IF FOUND RETURN ====
    if len(otherRss) > 0:
        #print "Other valid rss links:\n", "\n".join(otherRss)
        return otherRss
    else:
    #ELSE WE GO OPEN THE PAGES AND SEARCH THERE :O
        #print "\nOpen other rss links:\n", "\n".join(otherRssLike)
        for otherPage in otherRssLike:
            if len(g) > 3:
                return g
            g += goGetRss(otherPage, depth-1, searched+1) + [toHttp(i["href"], page) for i in rssxml]
    return g
    #return []

#==== ==== ==== ====
#RETURN RSS AS HTML WITH LINK
#==== ==== ==== ====
def index(req, link=""):
    return "\n".join(['<a href="%s">%s</a>' % (l, l) for l in goGetRss(rootHttpAdress(link), 3)])

print "Found rss:", goGetRss("http://www.kvarkadabra.net/", 2)
#print index(None, )
