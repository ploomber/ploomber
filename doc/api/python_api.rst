Python API
==========

DAG
---

.. currentmodule:: ploomber

.. autosummary::
    :toctree: _modules/root

    DAG
    OnlineDAG
    DAGConfigurator
    InMemoryDAG

.. _tasks-list:

Tasks
-----

.. currentmodule:: ploomber.tasks

.. autosummary::
    :toctree: _modules/tasks

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
    :toctree: _modules/products

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
    :toctree: _modules/clients

    Client
    DBAPIClient
    SQLAlchemyClient
    ShellClient


Spec
----

.. currentmodule:: ploomber.spec

.. autosummary::
    :toctree: _modules/spec

    DAGSpec



Env
---

.. currentmodule:: ploomber

.. autosummary::
    :toctree: _modules/root

    with_env
    load_env
    Env


SourceLoader
------------

.. currentmodule:: ploomber

.. autosummary::
    :toctree: _modules/root

    SourceLoader
