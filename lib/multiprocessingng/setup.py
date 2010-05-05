#!/usr/bin/python

__revision__ = "$Id$"
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
__maintainer__ = "John R. Frank"

from setuptools import setup

setup(name="multiprocessingng",
      version=__version__,
      description="Wrapper around multiprocessing to fix broken parts.",
      long_description="""\
Wrap multiprocessing.  Fix broken parts, allowing for resiliency in face of error,
and also making subclassing multiprocessing easier for changing functionality.
""",
      classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "License :: OSI Approved :: MIT License",
        ],
      keywords="python, persistent queue, persistent priority queue",
      author="MetaCarta, Inc.",
      author_email="labs@metacarta.com",
      url="http://code.google.com/p/pycrawler/",
      license="MIT",
      zip_safe=False,
      setup_requires=['coverage>=3.3', 'nose>=0.11'],
      packages=["multiprocessingng"],
      )

