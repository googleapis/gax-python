Google API Extensions for Python
================================

Google API Extensions for Python (gax-python) is a set of libraries which 
aids the development of APIs for clients and servers based on `GRPC`_ and 
Google API conventions.

Application code will rarely need to use most of the classes within this
library directly, but code generated automatically from the API definition
files can use services such as page streaming and request bundling to provide
a more convenient and idiomatic API surface to callers.


Prerequisite
------------

* Prior to grpc 0.11.0, the c-grpc runtime needs to be installed.

(optional) Remove any previous installation:

  ::

     brew uninstall grpc  # Will fail if brew not installed
     rm -rf ~/.linuxbrew
     sudo rm -fR /usr/local/include/grpc*
     sudo rm -fR /usr/local/lib/libgrpc*
     sudo rm -fR /usr/local/lib/libgpr*
     rm -fR /usr/local/lib/pkgconfig/grpc*.pc
     rm -fv /usr/local/lib/pkgconfig/gpr*.pc

Install linuxbrew:

  ::

     ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/linuxbrew/go/install)"
     export PATH=~/.linuxbrew/bin:${PATH}  # also modify your ~/.bashrc accordingly
     brew doctor

Install gRPC:

  ::

     curl -fsSL https://goo.gl/getgrpc | bash -
     curl -fsSL https://goo.gl/getgrpc | bash -s plugins


Installation
-------------


Install tox:

  ::

     sudo pip install tox  # done once, installed globally

Clone the repository (in this example, into ~/repos):

  ::

     mkdir ~/repos; cd ~/repos
     git clone git@github.com:googleapis/gax-python.git
     cd gcloud-python-gax
     CFLAGS=-I$(brew --prefix)/include LDFLAGS=-L$(brew --prefix)/lib tox -e py27
     tox -e py27


Developing
----------

Use tox

  ::

       $ cd ~/repos/gcloud-python-gax  # or wherever your repo is

       $ # Run the tests and linter (the default tox command)
       $ tox

       $ # Run the pep8 linter
       $ tox -e pep8


       $ # To run scripts, activate the development environment:
       $
       $ # Create a development virtualenv
       $ #
       $ # Set up a virtualenv at .tox/develop that contains the gcloud-python-gax package
       $ # with installed using python setup.py --develop.
       $ CFLAGS=-I$(brew --prefix)/include LDFLAGS=-L$(brew --prefix)/lib tox -e devenv

       $ # This puts the console scripts on the path
       $ . .tox/develop/bin/activate

       $(develop) # Later deactivate it
       $(develop) deactivate


Python Versions
---------------

gax-python is currently tested with Python 2.7.


Contributing
------------

Contributions to this library are always welcome and highly encouraged.

See the `CONTRIBUTING`_ documentation for more information on how to get started.


Versioning
----------

This library follows `Semantic Versioning`_ 

It is currently in major version zero (``0.y.z``), which means that anything
may change at any time and the public API should not be considered
stable.


Details
-------

For detailed documentation of the modules in gax-python, please watch `DOCUMENTATION`_. 


License
-------

BSD - See `LICENSE`_ for more information.

.. _`CONTRIBUTING`: https://github.com/googleapis/gax-python/blob/master/CONTRIBUTING.rst
.. _`LICENSE`: https://github.com/googleapis/gax-python/blob/master/LICENSE
.. _`GRPC`: http://grpc.io
.. _`Install virtualenv`: http://docs.python-guide.org/en/latest/dev/virtualenvs/
.. _`pip`: https://pip.pypa.io
.. _`gRPC protocol`: https://github.com/grpc/grpc-common/blob/master/PROTOCOL-HTTP2.md
.. _`edit RST online`: http://rst.ninjs.org
.. _`RST cheatsheet`: http://docutils.sourceforge.net/docs/user/rst/cheatsheet.txt
.. _`py.test`: http://pytest.org
.. _`Tox-driven python development`: http://www.boronine.com/2012/11/15/Tox-Driven-Python-Development/
.. _`Sphinx documentation example`: http://sphinx-doc.org/latest/ext/example_google.html
.. _`hyper`: https://github.com/lukasa/hyper
.. _`Google APIs`: https://github.com/google/googleapis/
.. _`Semantic Versioning`: http://semver.org/
.. _`DOCUMENTATION`: https://gax-python.readthedocs.org/
