#!/usr/bin/env python2.6

"""
Logging for pycrawler
"""

__author__ = "Joseph Malicki"
__copyright__ = "Copyright 2010, MetaCarta, Inc.  Copyright 2010, Nokia Corporation."
__license__ = "MIT License"
__version__ = "0.1"
__revision__ = "$Id$"

def configure_logger():
    """ initial logger configuration.  can be overridden. """
    import logging
    import logging.handlers
    import syslog

    global logger

    logger = logging.getLogger('PyCrawler')
    logger.setLevel(logging.INFO)

    handler = logging.handlers.SysLogHandler(facility=syslog.LOG_LOCAL0, address='/dev/log')
    formatter = logging.Formatter('%(filename)s[%(process)d]: %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    for h in logger.handlers:
        if h is not handler:
            logger.removeHandler(h)
    
    logger.propagate = False
    
    return logger

configure_logger()
