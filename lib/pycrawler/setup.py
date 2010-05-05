#!/usr/bin/python2.6

__revision__ = "$Id$"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
__maintainer__ = "John R. Frank"

from setuptools import setup

setup(name="PyCrawler",
      version=__version__,
      description="A python-based distributed web crawler.",
      long_description="""
Provides a web crawler.
""",
      classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "License :: OSI Approved :: MIT License",
        ],
      keywords="python, crawler",
      author="John R. Frank",
      author_email="postshift gmail",
      url="http://code.google.com/p/pycrawler/",
      license="MIT",
      zip_safe=False,
      install_requires=['pycurl',
                        'PersistentQueue',
                        'multiprocessingng',
                        'daemon',
                        'robotexclusionrulesparser',
                        'simplejson', 
                        'setproctitle',
                        ],

      setup_requires=['nose>=0.11'],
      tests_require=['coverage>=3.3'],

      entry_points="""
      # -*- Entry points: -*-
      """,
      packages=["PyCrawler"],
      )
