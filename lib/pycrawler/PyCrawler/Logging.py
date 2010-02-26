#!/usr/bin/python2.6
"""
Logging for pycrawler
"""
#$Id: __init__.py 129 2010-01-19 05:55:49Z postshift@gmail.com $
__author__ = "Joseph Malicki"
__copyright__ = "Copyright 2010, MetaCarta, Inc."
__license__ = "MIT License"
__version__ = "0.1"

def __configure_logger__():
    """ initial logger configuration.  can be overridden. """
    import logging
    import logging.handlers
    import syslog

    global logger

    logger = logging.getLogger('PyCrawler')
    logger.setLevel(logging.INFO)

    handler = logging.handlers.SysLogHandler(facility=syslog.LOG_LOCAL0)
    logger.addHandler(handler)

__configure_logger__()
