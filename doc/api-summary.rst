API Summary
===========

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
