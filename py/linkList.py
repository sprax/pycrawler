#==== ==== ==== ====
#TITLE + LINK
#==== ==== ==== ====
#import urllib

links = [("350 piglets die in truck crash", "http://edition.cnn.com/2008/WORLD/europe/06/11/germany.pigs.ap/index.html?eref=edition"),
         ("Google says it would support U.S. privacy law", "http://www.reuters.com/article/internetNews/idUSN1038231320080610?feedType=RSS&feedName=internetNews"),
         ("Dozens Killed In Sudan Plane Crash", "http://www.cbsnews.com/stories/2008/06/10/world/main4170047.shtml"),
         ("Ruby TMTOWTDI, Episode 1", "http://blog.citrusbyte.com/2008/6/2/ruby-tmtowtdi-episode-1"),
         ("Here Comes the Sun. So Watch Out", "http://news.yahoo.com/s/bw/20080611/bs_bw/jun2008bw20080610213588"),
         ("New York is Hillary Country", "http://www.scripting.com/stories/2008/06/04/newYorkIsHillaryCountry.html"),
         ("Gallery: The iPhone 2.0 Keynote", "http://www.wired.com/gadgets/mac/multimedia/2008/06/gallery_wwdc"),
         ("Driving Business Success Through Workgroup Choice and Flexibility", "http://www.computerworld.com/action/whitepapers.do?command=viewWhitePaperDetail&contentId=9095918&source=rss_news10"),
         ("Space Wallpapers and Nebula Wallpapers", "http://www.smashingmagazine.com/2008/06/11/space-wallpapers-and-nebula-wallpapers/"),
         ("The Week in Politics", "http://www.time.com/time/nation/article/0,8599,1814676,00.html"),
         ("Why Can't Programmers.. Program?", "http://www.codinghorror.com/blog/archives/000781.html"),
         ("Japan rescuers search for quake missing", "http://edition.cnn.com/2008/WORLD/asiapcf/06/15/japan.earthquake/index.html"),
         ("New page with example scripts", "http://www.ailab.si/orange/forum/viewtopic.php?p=1352#1352")]

#for title, link in links:
#    name = link.split("/")[2].split(".")[1]
#    open("html/%s.html" %name, "w").write(urllib.urlopen(link).read())