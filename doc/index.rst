Welcome to ploomber's documentation!
====================================

.. include:: ../README.rst

Table of contents
=================

.. toctree::
   :maxdepth: 3

   intro
   api


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
    SQLScript
    SQLDump
    SQLTransfer
    SQLUpload
    PostgresCopyFrom
    DownloadFromURL
    Link
    Input
    BashCommand
    ShellScript


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


Utilities
---------

.. currentmodule:: ploomber

.. autosummary::
    :toctree:

    SourceLoader
    Env

..
  The include below will render an orphan: text, due to this bug:
  https://github.com/sphinx-doc/sphinx/issues/1545


.. include:: auto_examples/index.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
