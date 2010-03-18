# $Id$
__copyright__ = "Copyright 2009, John R. Frank"
__license__ = "MIT License"
__version__ = "0.1"
__maintainer__ = "John R. Frank"

from setuptools import setup

version = "0.1"

setup(name="PersistentQueue",
      version=version,
      description="FIFO and priority queue interfaces to a set of flat-files on disk",
      long_description="""\
Provides a PersistentQueue.FIFO and PersistentQueue.RecordFactory of non-memory-bound sorting routines.   PersistentQueue.BatchPriorityQueue provides an orderable storage mechanism for namedtuples that represent state of objects, such as hosts being crawled.""",
      classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "License :: OSI Approved :: MIT License",
        ],
      keywords="python, persistent queue, persistent priority queue",
      author="John R. Frank",
      author_email="postshift gmail",
      url="http://code.google.com/p/pycrawler/",
      license="MIT",
      zip_safe=False,
      install_requires=['blist'],
      entry_points="""
      # -*- Entry points: -*-
      """,
      setup_requires=['coverage>=3.3', 'nose>=0.11'],
      packages=["PersistentQueue"],
      )

