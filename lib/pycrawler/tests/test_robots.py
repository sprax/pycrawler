
import os.path
import cPickle as pickle
import robotparser
import logging

def test_robots():
    """ Test that we can parse robots.txt, and pickle it. """
    path = os.path.split(os.path.abspath(__file__))[0]
    data = open(os.path.join(path, "robots.txt")).read()
    logging.info("%d bytes data" % len(data))

    rp = robotparser.RobotFileParser()
    logging.info("%d pickled bytes of robotparser" % len(pickle.dumps(rp)))

    rp.parse(data.splitlines())
    assert not rp.can_fetch("", "/search")

    logging.info("%d pickled bytes of robotparser" % len(pickle.dumps(rp)))

    #print pickle.dumps(rp)


if __name__ == '__main__':
    test_robots()
