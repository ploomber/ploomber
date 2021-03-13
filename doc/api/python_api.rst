Python API
==========

DAG
---

.. currentmodule:: ploomber

.. autosummary::
    :toctree:

    DAG
    OnlineDAG
    DAGConfigurator
    InMemoryDAG

.. _tasks-list:

Tasks
-----

.. currentmodule:: ploomber.tasks

.. autosummary::
    :toctree:

    Task
    PythonCallable
    NotebookRunner
    SQLScript
    SQLDump
    SQLTransfer
    SQLUpload
    PostgresCopyFrom
    ShellScript
    DownloadFromURL
    Link
    Input


Products
--------

.. currentmodule:: ploomber.products

.. autosummary::
    :toctree:

    Product
    File
    SQLRelation
    PostgresRelation
    SQLiteRelation
    GenericSQLRelation
    GenericProduct


Clients
-------

.. currentmodule:: ploomber.clients

.. autosummary::
    :toctree:

    Client
    DBAPIClient
    SQLAlchemyClient
    ShellClient


Spec
----

.. currentmodule:: ploomber.spec

.. autosummary::
    :toctree:

    DAGSpec



Env
---

.. currentmodule:: ploomber

.. autosummary::
    :toctree:

    with_env
    load_env
    Env


SourceLoader
------------

.. currentmodule:: ploomber

.. autosummary::
    :toctree:

    SourceLoader
