
def setup_package():
    """ set-up module path for nose tests. """

    import sys
    import os.path

    # i.e. ~/pycrawler_checkout/lib/queues
    path = os.path.split(os.path.dirname(os.path.abspath(__file__)))[0]
    sys.path.insert(0, path)

