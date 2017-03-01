Google API Extensions for Python
================================

.. image:: https://img.shields.io/travis/googleapis/gax-python.svg
     :target: https://travis-ci.org/googleapis/gax-python

.. image:: https://img.shields.io/pypi/dw/google-gax.svg
     :target: https://pypi.python.org/pypi/google-gax

.. image:: https://readthedocs.org/projects/gax-python/badge/?version=latest
     :target: http://gax-python.readthedocs.org/

.. image:: https://img.shields.io/codecov/c/github/googleapis/gax-python.svg
     :target: https://codecov.io/github/googleapis/gax-python


Google API Extensions for Python (gax-python) is a set of modules which aids the
development of APIs for clients and servers based on `gRPC`_ and Google API
conventions.

Application code will rarely need to use most of the classes within this library
directly, but code generated automatically from the API definition files in
`Google APIs`_ can use services such as page streaming and request bundling to
provide a more convenient and idiomatic API surface to callers.

.. _`gRPC`: http://grpc.io
.. _`Google APIs`: https://github.com/googleapis/googleapis/


Python Versions
---------------

gax-python is currently tested with Python 2.7, 3.4, 3.5, and 3.6.


Contributing
------------

Contributions to this library are always welcome and highly encouraged.

See `CONTRIBUTING.rst`_ for more information on how to get started.

.. _CONTRIBUTING.rst: https://github.com/googleapi/gax-python/blob/master/CONTRIBUTING.rst

Versioning
----------

This library follows `Semantic Versioning`_

It is currently in major version zero (``0.y.z``), which means that anything
may change at any time and the public API should not be considered
stable.

.. _`Semantic Versioning`: http://semver.org/


Details
-------

For detailed documentation of the modules in gax-python, please watch `DOCUMENTATION`_.

.. _`DOCUMENTATION`: https://gax-python.readthedocs.org/


License
-------

BSD - See `the LICENSE`_ for more information.

.. _`the LICENSE`: https://github.com/googleapis/gax-python/blob/master/LICENSE
