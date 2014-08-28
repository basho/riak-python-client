Riak Python Client
==================

Tutorial
--------

The tutorial documentation has been converted to the `Basho Docs`_ as
the `Taste of Riak: Python`_. The old tutorial_ that used to live here
has been moved to the `Github Wiki`_ and is likely out-of-date.

.. _`Basho Docs`: http://docs.basho.com/
.. _`Taste of Riak: Python`:
   http://docs.basho.com/riak/latest/dev/taste-of-riak/python/
.. _tutorial:
   https://github.com/basho/riak-python-client/wiki/Tutorial-%28old%29
.. _`Github Wiki`: https://github.com/basho/riak-python-client/wiki

Installation
------------

#. Ensure Riak installed & running. (``riak ping``)
#. Install the Python client:

  #. If you use Pip_, ``pip install riak``.
  #. If you use easy_install_, run ``easy_install riak``.
  #. You can download the package off PyPI_, extract it and run
     ``python setup.py install``.

.. _Pip: http://pip.openplans.org/
.. _easy_install: http://pypi.python.org/pypi/setuptools
.. _PyPI: http://pypi.python.org/pypi/riak/

Development
-----------

All development is done on Github_. Use Issues_ to report
problems or submit contributions.

.. _Github: https://github.com/basho/riak-python-client/
.. _Issues: https://github.com/basho/riak-python-client/issues


Indices and tables
------------------

* :ref:`genindex`
* :ref:`search`

Contents
--------

.. toctree::
   :maxdepth: 1

   client
   bucket
   object
   datatypes
   query
   security
   advanced
