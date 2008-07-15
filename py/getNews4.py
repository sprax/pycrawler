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
def getNewsLength(title, link):
    html = urllib.urlopen(link).read()
    html = re.sub("\s{2,}", " ", html)
    html = re.sub("\n", " ", html)
    html = re.sub("<script.*?<\/script>|<style.*?<\/style>|<title.*?<\/title>", " ", html)
    html = re.sub("<\/?[div|td|tr|table].*?>", "\n", html)
    html = re.sub("<\/?.*?>", " ", html)

    foo = html.split(title.encode("UTF-8"))
    if len(foo) > 1:
        bra = re.split("[A-Z](\w+\s){4,}[^\t\.\?\!]*[a-z]\.\s[A-Z]", foo[1])
        ostanek = " ".join(bra[1:])
        o = ostanek.split("\n\n")
        return min((len(o[0]) / 1.2 + 100), 1500)
    return 60


#==== ==== ==== ====
#GET NEWS
#==== ==== ==== ====
def getNews(title, link):
    #==== ==== ==== ====
    #OPEN
    #==== ==== ==== ====
    #title, link = (u"Njavro in Krkovič nista pričala", "http://www.delo.si/clanek/62212")
    name = link.split("/")[2].split(".")[1]
    p = urllib.urlopen(link).read()
    #p = open("html/%s.html" % name).read()
    amountExpected = getNewsLength(title,link)
    print "EXPECTED:", amountExpected
    p = BeautifulSoup(repair(pretty(p)))
    #TODO: Better title find
    titleNode = p.find(text=title)
    s = titleNode
    up = 5
    #WHILE WE DONT FIND ENOUGHT TEXT OR WE DID GO TO HIGH UP
    #==== ==== ==== ====
    #1. IF ONE SIBLING HAS ENOUGHT TEXT,
    #   FIND HIS CORE SUPER DADDY
    #==== ==== ==== ====
    #print "$" * 5, "1", "$" * 5
    while up > 0 and titleNode != None:
        #If it has a sibling, else go parant
        try:
            s = [titleNode.nextSibling, titleNode.parent][titleNode.nextSibling == None]

            #==== ==== ==== ====
            #1. IF ONE SIBLING HAS ENOUGHT TEXT,
            #   FIND HIS CORE SUPER DADDY
            #==== ==== ==== ====
            for i in s.fetchNextSiblings():
                #print "=" * 5, "\n", i, "=" * 5, "\n" * 3
                if i != None:
                    if len(" ".join(getText(i))) > amountExpected:
                        superDaddy = findSuperDaddy(i, amountExpected)
                        #print "-" * 5, "\n", superDaddy, "-" * 5, "\n" * 3
                        print "#1 vraca"
                        #print "\n".join(getText(superDaddy))
                        return "\n".join([str(i) for i in superDaddy.contents if getTagName(i) != "div"])

            #==== ==== ==== ====
            #2. IF SIBLINGS HAVE ENOUGHT TEXT
            #==== ==== ==== ====
            #print "$" * 5, "2", "$" * 5
            t = [getText(i) for i in s.fetchNextSiblings() if i != None]

            if sum([len(" ".join(i)) for i in t]) > amountExpected:
                print "#2 vraca"
                return "\n".join([str(i) for i in s.fetchNextSiblings() if i != None and getTagName(i) != "div"])

            #==== ==== ==== ====
            #3. IF CURRENT NODE HAS ENOUGHT TEXT
            #==== ==== ==== ====
            t = [i for i in s.contents if isinstance(i, unicode)]

            if len(" ".join(t)) > amountExpected:
                print "#3 vraca"
                return "\n".join(map(str, s.contents))
        except Exception, inst:
            print "\n".join([str(type(inst)), str(inst.args), str(inst)])

        titleNode = titleNode.parent

    return ""
    #print "\nNAME:", name


def removeDiv(html):
    return re.sub("<div.*?<\/div>", " ", html)

for title, link in links:
    name = link.split("/")[2].split(".")[1]
    print "--", name, title
    open("out/%s.html" % name, "w").write("<title>%s</title>\n%s" %(name, removeDiv(getNews(title, link))))
    print " "