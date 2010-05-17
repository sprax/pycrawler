
Web crawler. Requires python 2.6.x.

Currently split into three parts:

lib/queues:
  Contains PersistentQueue library.
lib/pycrawler:
  Contains PyCrawler module.  depends on PersistentQueue.
webui: 
  Contains PyCrawlerWebUI module.  depends on PyCrawler.

RECOMMENDED ENVIRONMENT
=========================
install virtualenv, and add virtualenv directory's bin/ to path
in .profile (or whatever your environment uses).

PATCHES
=========================
Released versions coverage.py and nose's coverage plugins do not
properly support software that spawns multiple processes via fork.
To get coverage information across all processes requires the following:

Requires python coverage.py from trunk (post-3.3.1, until 3.3.2 is released):
 $ hg clone http://bitbucket.org/ned/coveragepy
 $ cd coveragepy
 $ python setup.py install

Also requires a patched nose:
 see docs/README 

Alternatively, one can comment out the 'cover_parallel' options
in the setup.cfg's.

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
