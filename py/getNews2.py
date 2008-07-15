#==== ==== ==== ====
#GET NEWS v2
#Using a more complex algoritem
#It finds the first big amount of text that is close to the title
#If the ammount is set right u will get the correct text
#==== ==== ==== ====
import urllib
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

#==== ==== ==== ====
#GET TEXT ONLY FROM ELEMENT
#==== ==== ==== ====
def getText(node):
    return [e for e in node.recursiveChildGenerator() if isinstance(e, unicode)]

def getTagName(i):
    try:
        return i.name
    except:
        return "None"


#==== ==== ==== ====
#SUPER DADDY: IS THE THE DADDY THAT
#IS THE FIRST COMMON, THAT HAS ENOUGHT TEXT IN HIM
#==== ==== ==== ====
def findSuperDaddy(node, amount=500):
    try:
        for e in node.contents:
            k = findSuperDaddy(e, amount)
            if k:
                return k
        if len(" ".join(getText(node))) > amount:
            #print "$" * 2, len(" ".join(getText(node))), "\n", " ".join(getText(node)), "\n" * 2
            #print "node:", node
            return node
        return None
    except:
        return None

#==== ==== ==== ====
#GET NEWS LENGTH - very inconsistent
#==== ==== ==== ====
def getNewsLength(title, link, html):
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
#OPEN
#==== ==== ==== ====
title, link = ("350 piglets die in truck crash", "http://edition.cnn.com/2008/WORLD/europe/06/11/germany.pigs.ap/index.html?eref=edition")
name = link.split("/")[2].split(".")[1]
p = urllib.urlopen(link).read()
#p = open("html/%s.html" % name).read()
amountExpected = 416
p = BeautifulSoup(repair(pretty(p)))
#TODO: Better title find
titleNode = p.findAll(text=title)[0]
s = titleNode.parent.parent.parent

#==== ==== ==== ====
#1. IF ONE SIBLING HAS ENOUGHT TEXT,
#   FIND HIS CORE SUPER DADDY
#==== ==== ==== ====
print "$" * 5, "1", "$" * 5
for i in s.fetchNextSiblings():
    #print "=" * 5, "\n", i, "=" * 5, "\n" * 3
    if i != None:
        if len(" ".join(getText(i))) > amountExpected:
            superDaddy = findSuperDaddy(i, amountExpected)
            #print "-" * 5, "\n", superDaddy, "-" * 5, "\n" * 3
            print "#" * 10
            print "\n".join([str(i) for i in superDaddy.contents if getTagName(i) != "div"])
            print "#" * 10
            break

#==== ==== ==== ====
#2. IF SIBLINGS HAVE ENOUGHT TEXT
#==== ==== ==== ====
print "$" * 5, "2", "$" * 5
t = [getText(i) for i in s.fetchNextSiblings() if i != None]

if sum([len(" ".join(i)) for i in t]) > amountExpected:
    print "\n".join([str(i) for i in s.fetchNextSiblings() if i != None and getTagName(i) != "div"])

#==== ==== ==== ====
#3. IF CURRENT NODE HAS ENOUGHT TEXT
#==== ==== ==== ====
print "$" * 5, "3", "$" * 5
t = [i for i in s.contents if isinstance(i, unicode)]

if len(" ".join(t)) > amountExpected:
    print "\n\n".join(t)

print "\nNAME:", name