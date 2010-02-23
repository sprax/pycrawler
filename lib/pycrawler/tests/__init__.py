
def setup_package():
    """ set-up module path for nose tests. """

    import sys
    import os.path

    # i.e. ~/pycrawler_checkout/lib/pycrawler
    path = os.path.split(os.path.dirname(os.path.abspath(__file__)))[0]
    queues = os.path.join(os.path.split(path)[0], 'queues')

    sys.path.insert(0, queues)
    sys.path.insert(0, path)

