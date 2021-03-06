#!/usr/bin/python
# -*- coding: utf-8 -*-

__revision__ = "$Id$"

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name='PyCrawlerWebUI',
    version='0.1',
    description='',
    author='',
    author_email='',
    #url='',
    install_requires=[
        "TurboGears2 >= 2.0b7",
        "Catwalk >= 2.0.2",
        "Babel >=0.9.4",
        #can be removed iif use_toscawidgets = False
        "toscawidgets >= 0.9.7.1",
        "zope.sqlalchemy >= 0.4 ",
        "repoze.tm2 >= 1.0a4",
        "routes < 1.12", # 1.12 is broken

        # Pylons 0.10rc1 deprecates TurboGears 2.0.3's use of pylons.
        "Pylons < 0.10",
        # Not strictly needed for production, but needed for development,
        # and no good way to get those dependencies.
        "tg.devtools",
                        ],
    setup_requires=["PasteScript >= 1.7"],
    paster_plugins=['PasteScript', 'Pylons', 'TurboGears2', 'tg.devtools'],
    packages=find_packages(exclude=['ez_setup']),
    include_package_data=True,
    test_suite='nose.collector',
    tests_require=['WebTest', 'BeautifulSoup'],
    package_data={'PyCrawlerWebUI': ['i18n/*/LC_MESSAGES/*.mo',
                                 'templates/*/*',
                                 'public/*/*']},
    message_extractors={'PyCrawlerWebUI': [
            ('**.py', 'python', None),
            ('templates/**.mako', 'mako', None),
            ('templates/**.html', 'genshi', None),
            ('public/**', 'ignore', None)]},

    entry_points="""
    [paste.app_factory]
    main = PyCrawlerWebUI.config.middleware:make_app

    [paste.app_install]
    main = pylons.util:PylonsInstaller
    """,
)
