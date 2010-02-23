
import os.path
import cPickle as pickle
import robotparser

def test_robots():
    path = os.path.split(os.path.abspath(__file__))[0]
    data = open(os.path.join(path, "robots.txt")).read()
    print "%d bytes data" % len(data)

    rp = robotparser.RobotFileParser()
    print "%d pickled bytes of robotparser" % len(pickle.dumps(rp))

    rp.parse(data.splitlines())
    assert not rp.can_fetch("", "/search")

    print "%d pickled bytes of robotparser" % len(pickle.dumps(rp))

    #print pickle.dumps(rp)


if __name__ == '__main__':
    test_robots()
