Python API
==========

DAG
---

.. currentmodule:: ploomber

.. autosummary::
    :toctree:

    DAG
    DAGConfigurator
    InMemoryDAG

Tasks
-----

.. currentmodule:: ploomber.tasks

.. autosummary::
    :toctree:

    Task
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

    Product
    File
    PostgresRelation
    SQLiteRelation
    GenericProduct
    GenericSQLRelation


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
