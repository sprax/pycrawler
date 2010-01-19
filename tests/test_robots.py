import cPickle as pickle
import robotparser

data = open("robots.txt").read()
print "%d bytes data" % len(data)

rp = robotparser.RobotFileParser()
print "%d pickled bytes of robotparser" % len(pickle.dumps(rp))

rp.parse(data.splitlines())
assert not rp.can_fetch("", "/search")

print "%d pickled bytes of robotparser" % len(pickle.dumps(rp))

#print pickle.dumps(rp)


