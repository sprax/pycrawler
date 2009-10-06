#==== ==== ==== ====
#A TEST ON A GENERIC ALGORITEM THAT WILL TRY TO GET NEWS TEXT
#FROM ANY NEWS SITE, NON SPECIFIC ALGORITEM
#1. FINDS TITLE IN TREE
#2. MOVES UP AND RIGHT IN DOM TREE TO FIND THE TEXT
#==== ==== ==== ====
import urllib
import xml.dom.minidom
import codecs
import re
from BeautifulSoup import BeautifulSoup
from prettySoup import pretty
from linkList import links
#==== ==== ==== ====
#SCRIPT + COMENTARY + CSS OUT - for Soup
#==== ==== ==== ====
def repair(html):
    html.replace("<p>", "[p]").replace("</p>", "[/p]")
    return re.sub("<script.*?<\/script>|<style.*?<\/style>|<!--.*?-->", " ", html)

#links = [("Irish EU vote lost, officials say", "http://news.bbc.co.uk/2/hi/europe/7452171.stm")]
progressFile = "d:/FAX/3. letnik/OS/WebCrawler/py/progressFile.txt"

#==== ==== ==== ====
#PARSE XML + CALL getNews
#==== ==== ==== ====
def getNewsFromRss(rsslink):

    rss = urllib.urlopen(rsslink)
    rssxml = xml.dom.minidom.parseString(rss.read())
    links = []
    wholeRss = []
    counter = 0
    #try:
    for item in rssxml.getElementsByTagName("item"):
        title = item.getElementsByTagName("title")[0].firstChild.data
        link = item.getElementsByTagName("link")[0].firstChild.data
        description = item.getElementsByTagName("description")[0].firstChild.data
        links.append([title, link])
        #PROGRESS FILE
        counter+=1
        open(progressFile, "w").write(str(counter) + "/" + str(len(links)))
        #GET NEWS
        wholeRss.append((title, link, getNews(title, link)))
        #print "=" * 10, "TITLE: %s\nTEXT: (%s)\n\nLINK: %s\n" % (title, description, link)
        #print "Text only:", re.sub("<[^>]*>+", "", description)
    #except Exception, inst:
    #    return "\n".join([str(type(inst)), str(inst.args), str(inst)])
    return wholeRss

#==== ==== ==== ====
#GET NEWS LENGTH - very inconsistent
#==== ==== ==== ====
def getNewsLength(title, link):
    name = link.split("/")[2].split(".")[1]
    html = urllib.urlopen(link).read()
    #html = open("html/%s.html" % name).read()
    html = re.sub("[\n{2,}\r]", "\t", html)
    html = re.sub("\s{2,}", " ", html)
    #ALL TAGS <..>, JS SCRIPTS, CSS, TITLE, HTML COMMENTS OUT
    html = re.sub("<script.*?<\/script>|<style.*?<\/style>|<title.*?<\/title>|<.*?>", "\t", html)
    #From title
    html = html[html.find(title)+len(title):]
    sentences = [i for i in re.findall("([A-Z]\w+\s\w+\s[^\t\.\?\!]*\.\s){2}", html) if len(i) > 30] #\s[A-Z]\w+\s[^\t\.\?\!]*\.
    #name = link.split("/")[2].split(".")[1]
    #open(name + ".html", "w").write(title + "\n" + "\n".join(sentences))
    #open(name + ".html", "w").write("\n" * 3 + title + "\n" + html)
    return min((sum([len(i) for i in sentences]) / 2.5 + 100), 1800)

#==== ==== ==== ====
#GET NEWS
#==== ==== ==== ====
def getNews(title, link):
    #OPEN PAGE
    #p = urllib.urlopen(link).read()
    p = open("html/%s.html" % (link.split("/")[2].split(".")[1])).read()
    p = BeautifulSoup(repair(pretty(p)))
    #VIA TITLE, GET ENOUGHT TEXT, MAX 6 LEVELS UP
    #TODO: More advance title finding, since sometimes its just a substring
    titleNode = p.find(text=title)
    text = ""
    up = 5
    #WHILE WE DONT FIND ENOUGHT TEXT OR WE DID GO TO HIGH UP
    #TODO: MINIMUN CHARACTER GET STATISTICLY :p
    minLenght = getNewsLength(title, link)
    while len(text) < minLenght and up > 0 and titleNode != None:
        #If it has a sibling, else go parant
        s = [titleNode.nextSibling, titleNode.parent][titleNode.nextSibling == None]

        print "=" * 5, "1:s:::", s
        #GO OVER ALL SIBLING and SEARCH FOR TEXT
        while s:
            try:
                text = "\n".join(s.findChildren(text=lambda(x): len(x) > 30  and re.compile(".*[A-Z]\w+\s\w+\s[^\t\.\?]*\.*").match(x)))
                print "=" * 5, "2:findChildren :::", s.findChildren(text=lambda(x): len(x) > 30 and re.compile(".*[A-Z]\w+\s\w+\s[^\t\.\?]*\.*").match(x))
                print "=" * 5, "3:len(text):::", len(text)
                if len(text) > minLenght:
                    print "=" * 10
                    print s
                    break
            except Exception, inst:
                #print "\n".join([str(type(inst)), str(inst.args), str(inst)])
                break
            finally:
                s = s.nextSibling
        #GO UP
        titleNode = titleNode.parent
        up-=1
        #print "\n================\ntitleNode:", titleNode
    print "\nTEXT:\n", text
    return text

#print "\n".join(["%s: %s\n%s\n\n" % (t, l, text[:100]) for t, l, text in getNewsFromRss("http://www.forzanka.si/rss.xml")])
#open("out.html", "wb").write("\n".join(["%s: %s\n%s" % (t, l, text) for t, l, text in getNewsFromRss("http://www.forzanka.si/rss.xml")]))
for title, link in links:
    print "\n", "#" * 10, title, link, "#" * 10, "\n" * 2
    #getNews(title, link)

def index(req, rssLink="http://www.kvarkadabra.net/backend/kvarkadabra.rss"):
    html = "<table>"
    for title, link, text in getNewsFromRss(rssLink):
        html += "<tr><td>TITLE: <a href='%s'>%s</a></td></tr>\n" % (link, title)
        html += "<tr><td>%s</td></tr>\n" % 1 #getNewsLength(title, link)[1]
        html += "<tr><td>TEXT: %s<br/></td></tr>\n" % text
        html += "<tr><td>--</td></tr>\n"
    return html + "</table>"
