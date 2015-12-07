google-gax
==========

google-gax is the Google API extension library.

Status
------

* Initial checkin
* Python open source project scaffolding


Prerequisite
------------

* grpcio needs to be installed


Installation
-------------

* At the moment, it is unpublished and needs to be installed from source
  following checkout from its git repo.  See developing below.


Developing
----------

Use tox

  ::

       $ sudo pip install tox  # done once, installed globally
       $
       $ # Run the tests and linter (the default tox command)
       $ tox
       $
       $ # Run the pep8 linter
       $
       $ tox -e pep8
       $
       $ ...
       $ # To run scripts, activate the development:
       $
       $ # Create a development virtualenv
       $ #
       $ # Set up a virtualenv at .tox/develop that contains the nurpc-hyper.protocol package
       $ # with installed using python setup.py --develop.
       $
       $ tox -e devenv
       $
       $ # This puts the console scripts on the path
       $ . .tox/develop/activate
       $
       $(develop) # Later deactivate it
       $(develop) deactivate

.. _`Install virtualenv`: http://docs.python-guide.org/en/latest/dev/virtualenvs/
.. _pip: https://pip.pypa.io
.. _`gRPC protocol`: https://github.com/grpc/grpc-common/blob/master/PROTOCOL-HTTP2.md
.. _`edit RST online`: http://rst.ninjs.org
.. _`RST cheatsheet`: http://docutils.sourceforge.net/docs/user/rst/cheatsheet.txt
.. _`py.test`: http://pytest.org
.. _`Tox-driven python development`: http://www.boronine.com/2012/11/15/Tox-Driven-Python-Development/
.. _`Sphinx documentation example`: http://sphinx-doc.org/latest/ext/example_google.html
.. _`hyper`: https://github.com/lukasa/hyper
