.. include:: ../README.rst

.. include:: features.rst

..
  The include below will render an orphan: text, due to this bug:
  https://github.com/sphinx-doc/sphinx/issues/1545


.. include:: auto_examples/index.rst


API
===

DAG
---

.. currentmodule:: ploomber

.. autosummary::
    :toctree:

    DAG

Tasks
-----

.. currentmodule:: ploomber.tasks

.. autosummary::
    :toctree:

    PythonCallable
    NotebookRunner
    ShellScript
    SQLScript
    SQLDump
    SQLTransfer
    SQLUpload
    PostgresCopyFrom
    DownloadFromURL
    Link
    Input


Products
--------

.. currentmodule:: ploomber.products

.. autosummary::
    :toctree:

    File
    PostgresRelation
    SQLiteRelation

Clients
-------

.. currentmodule:: ploomber.clients

.. autosummary::
    :toctree:

    DBAPIClient
    SQLAlchemyClient
    ShellClient


Utilities
---------

.. currentmodule:: ploomber

.. autosummary::
    :toctree:

    SourceLoader
    Env


Table of contents
=================

.. toctree::
   :maxdepth: 3

   api


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
