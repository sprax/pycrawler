# $Id$
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
__maintainer__ = "John R. Frank"

from setuptools import setup

version = "0.1"

setup(name="PersistentQueue",
      version=version,
      description="queue and heap-like data structures that persist data in flat-files on disk",
      long_description="""\
Provides a PersistentQueue.Queue that is a persistent FIFO that includes a sorting mechanism for re-ordering the data into an ordered (heap-like) structure.   PersistentQueue.TriQueue provides persistence with state.  Records never actually leave the TriQueue.  Rather they are set to pending until the next sync.""",
      classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "License :: OSI Approved :: MIT License",
        ],
      keywords="python, persistent queue, persistent heap",
      author="John R. Frank",
      author_email="postshift gmail",
      url="http://code.google.com/p/pycrawler/",
      license="MIT",
      zip_safe=False,
      install_requires=[],
      entry_points="""
      # -*- Entry points: -*-
      """,
      packages=["PersistentQueue"],
      )

