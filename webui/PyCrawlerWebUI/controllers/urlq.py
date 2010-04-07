# -*- coding: utf-8 -*-
"""URL submission Controller"""

from tg import expose, flash, require, url, request, redirect
from pylons.i18n import ugettext as _, lazy_ugettext as l_

from PyCrawlerWebUI.lib.base import BaseController
from PyCrawlerWebUI.model import DBSession, metadata
from PyCrawlerWebUI.controllers.error import ErrorController

from tw.forms import TableForm, TextField
from tw.api import WidgetsList

from formencode.validators import URL as URLValidator

from PyCrawler import FetchClient

__all__ = ['URLQController']

class URLForm(TableForm):
    class fields(WidgetsList):
        url = TextField(validator=URLValidator)

url_form = URLForm("url_form", action="/urlq/submitseed")

class URLQController(BaseController):
    """
    
    """

    error = ErrorController()

    @expose('PyCrawlerWebUI.templates.urlq')
    def index(self):
        """Handle the front-page."""
        return dict(page = 'urlq',
                    url_form = url_form,
                    )

    @expose()
    def submitseed(self, url=None):
        fetch_client = FetchClient()
        fetch_client.add_url(url)
        # FIXME. should be relative.
        flash('Added seed %s' % url)
        raise redirect('/urlq')
