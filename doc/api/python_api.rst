Python API
==========

This section lists the available classes and functions in the Python API. If
you're writing pipelines with the Spec API (e.g., ``pipeline.yaml`` file), you
won't interact with this API directly. However, you may still want to learn
about :class:`ploomber.spec.DAGSpec` if you need to load your pipeline as a Python
object.

For code examples using the Python API, `click here <https://github.com/ploomber/projects/tree/master/python-api-examples>`_.

DAG
---

.. currentmodule:: ploomber

.. autosummary::
    :toctree: _modules/root

    DAG
    OnlineModel
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
    S3Client
    GCloudStorageClient


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

Serialization
-------------

.. currentmodule:: ploomber.io

.. autosummary::
    :toctree: _modules/io

    serializer
    serializer_pickle
    unserializer
    unserializer_pickle


Executors
---------

.. currentmodule:: ploomber.executors

.. autosummary::
    :toctree: _modules/executors

    Serial
    Parallel


SourceLoader
------------

.. currentmodule:: ploomber

.. autosummary::
    :toctree: _modules/root

    SourceLoader
