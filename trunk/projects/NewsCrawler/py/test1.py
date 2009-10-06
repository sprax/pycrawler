#==== ==== ==== ====
#GETTING TEXT ONLY FROM HTML, TO GET TEXT THAT WE NEED
#==== ==== ==== ====

import urllib
import re
from math import log
from linkList import links
#==== ==== ==== ====
#GET NEWS LENGTH - very inconsistent
#==== ==== ==== ====
def getNewsLength(title, link):
    name = link.split("/")[2].split(".")[1]
    html = urllib.urlopen(link).read()
    html = re.sub("[\n{2,}\r]", "\t", html)
    html = re.sub("\s{2,}", " ", html)
    #ALL TAGS <..>, JS SCRIPTS, CSS, TITLE, HTML COMMENTS OUT
    html = re.sub("<script.*?<\/script>|<style.*?<\/style>|<title.*?<\/title>|<.*?>", "\t", html)
    #From title
    html = html[html.find(title)+len(title):]
    sentences = [i for i in re.findall("([A-Z]\w+\s\w+\s[^\t\.\?\!]*\.\s){2}", html) if len(i) > 30] #\s[A-Z]\w+\s[^\t\.\?\!]*\.
    #name = link.split("/")[2].split(".")[1]
    #open("test/" + name + ".html", "w").write(title + "\n" + "\n".join(sentences))
    #open("test/" + name + ".html", "w").write("\n" * 3 + title + "\n" + html)
    return min((sum([len(i) for i in sentences]) / 3 + 150), 1800)

#==== ==== ==== ====
#LETS GET TEXT ONLY
#Starting with the title
#==== ==== ==== ====
for title, link in links:
    name = link.split("/")[2].split(".")[1]
    l = getNewsLength(title, link)
    print name, l

