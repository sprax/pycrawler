import re
import urllib
from linkList import links

def izpljuni(i):
    open("c:/out.html", "w").write(i)

title, link = (u"Njavro in Krkovič nista pričala", "http://www.delo.si/clanek/62212")


for title, link in [("Dozens Killed In Sudan Plane Crash", "http://www.cbsnews.com/stories/2008/06/10/world/main4170047.shtml")]:
    html = urllib.urlopen(link).read()
    html = re.sub("\s{2,}", " ", html)
    html = re.sub("\n", " ", html)
    html = re.sub("<script.*?<\/script>|<style.*?<\/style>|<title.*?<\/title>", " ", html)
    html = re.sub("<\/?[div|td|tr|table].*?>", "\n", html)
    html = re.sub("<\/?.*?>", " ", html)

    foo = html.split(title.encode("UTF-8"))
    if len(foo) > 1:
        bra = re.split("[A-Z](\w+\s){2,}[^\t\.\?\!]*[a-z]\.\s", foo[1])
        ostanek = " ".join(bra[1:])
        o = ostanek.split("\n\n")
        print len(o[0]), title, link
        print o[0]

#izpljuni(foo[1])