# -*- coding: utf-8 -*-
"""Main Controller"""

from tg import expose, flash, require, url, request, redirect
from pylons.i18n import ugettext as _, lazy_ugettext as l_

from PyCrawlerWebUI.lib.base import BaseController
from PyCrawlerWebUI.model import DBSession, metadata
from PyCrawlerWebUI.controllers.error import ErrorController
from PyCrawlerWebUI.controllers.urlq import URLQController

__all__ = ['RootController']


class RootController(BaseController):
    """
    The root controller for the PyCrawlerWebUI application.
    
    All the other controllers and WSGI applications should be mounted on this
    controller. For example::
    
        panel = ControlPanelController()
        another_app = AnotherWSGIApplication()
    
    Keep in mind that WSGI applications shouldn't be mounted directly: They
    must be wrapped around with :class:`tg.controllers.WSGIAppController`.
    
    """
    
    error = ErrorController()
    urlq = URLQController()

    @expose('PyCrawlerWebUI.templates.index')
    def index(self):
        """Handle the front-page."""
        return dict(page='index')

    @expose('PyCrawlerWebUI.templates.about')
    def about(self):
        """Handle the 'about' page."""
        return dict(page='about')

