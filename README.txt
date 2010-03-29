
Web crawler. Requires python 2.6.x.

Currently split into three parts:

lib/queues:
  Contains PersistentQueue library.
lib/pycrawler:
  Contains PyCrawler module.  depends on PersistentQueue.
webui: 
  Contains PyCrawlerWebUI module.  depends on PyCrawler.

INSTALLING
=========================

In each there is a setup.py.  To test:
  python2.6 setup.py nosetests
To install:
  python2.6 setup.py install

The "setup.py nosetests" command will download and build all prerequisite
packages to the local directory, the only exception being the build toolchain,
including libcurl.

Once this is done, setup.py install can be done as root if needed, or a user
can use virtualenv to install to a local directory.
