SQL pipelines
=============

This guide explains how to develop pipelines where some of all tasks are SQL scripts.

Quick Start
-----------

If you want to take a look at the sample pipeline, you have a few options:

- `Source code on Github <https://github.com/ploomber/projects/tree/master/spec-api-sql>`_
- `Interactive demo <https://mybinder.org/v2/gh/ploomber/projects/master?filepath=spec-api-sql%2FREADME.md>`_

Or run it locally:


.. code-block:: console

    ploomber examples --name spec-api-sql


Connecting to databases
-----------------------

The first step to write a SQL pipeline is to tell Ploomber how to connect to
the database, by providing a function that returns either a
:py:mod:`ploomber.clients.SQLAlchemyClient` or a
:py:mod:`ploomber.clients.DBAPIClient`. These two clients cover all databases
supported by Python, even systems like Snowflake or Apache
Hive.

``SQLAlchemyClient`` takes a single argument, the database URI. As the name
suggests, it uses SQLAlchemy under the hood, so any database supported by such
library is supported as well. Below, there's is an example that connects to
a local SQLite database:


.. code-block:: python
    :class: text-editor
    :name: clients-py

    from ploomber.clients import SQLAlchemyClient

    def get_client():
        return SQLAlchemyClient('sqlite:///database.db')


`Click here for documentation on database URIs <https://docs.sqlalchemy.org/en/13/core/engines.html>`_.


If your database isn't supported by SQLAlchemy, you must use
:py:mod:`ploomber.clients.DBAPIClient` instead. Refer to the documentation for
details.


Configuring the task client in ``pipeline.yaml``
------------------------------------------------

To configure your ``pipeline.yaml`` to run a SQL task, ``source`` must be a
path to the SQL script. To indicate how to load the client, you have to
include the ``client`` key:

.. code-block:: yaml
    :class: text-editor

    tasks:
        source: sql/create-table.sql
        product: [schema, name, table]
        client: clients.get_client


``product`` can be a list with three elements: ``[schema, name, kind]``,
or 2: ``[name, kind]``. Where kind can be ``table`` or ``view``.

``client`` must be a dotted path to a function that
instantiates a client. If your ``pipeline.yaml`` and ``clients.py`` are in the same
folder, you should be able to do this directly. If they are in a different
folder, you'll have to ensure that the function is importable.

You can reuse the same dotted path in many tasks. However, since it is
common for many tasks to query the same database, you may declare a task-level
client like this:

.. code-block:: yaml
    :class: text-editor

    clients:
        # all SQLScript tasks use the same client instance
        SQLScript: clients.get_client

    tasks:
        source: sql/create-table.sql
        product: [schema, name, table]
        # no need to add client here

Product's metadata
------------------

Incremental builds (:ref:`incremental-builds`) allow you speed-up pipeline
execution. To enable this, Ploomber keeps track of source code changes. When
tasks generate files (say ``data.csv``), a metadata file is saved next to
the product file (e.g., ``.data.csv.metadata``). To enable incremental builds
in SQL pipelines, you must configure a product metadata backend.

If you are using PostgreSQL, you can use
:py:mod:`ploomber.products.PostgresRelation`, if using SQLite, you can use
:py:mod:`ploomber.products.SQLiteRelation`. In both cases, metadata is saved
in the same database where the tables/views are created, hence, you can reuse
the task client. Here's an example if using PostgreSQL:


.. code-block:: yaml
    :class: text-editor
    :name: pipeline-pg-yaml

    meta:
        # configure pipeline to use PostgresRelation by default
        product_default_class:
            SQLScript: PostgresRelation

    # same client for task and product 
    clients:
        SQLScript: clients.get_pg_client
        PostgresRelation: clients.get_pg_client

    tasks:
        source: sql/create-table.sql
        product: [schema, name, table]


For any other database, you have two options, either use
:py:mod:`ploomber.products.SQLRelation` which is a product that does not save
any metadata at all (this means you don't get incremental builds) or use
:py:mod:`ploomber.products.GenericSQLRelation`, which stores metadata in a SQLite
database.

A typical configuration to enable incremental builds looks like this:

.. code-block:: yaml
    :class: text-editor
    :name: pipeline-generic-yaml
    
    meta:
        product_default_class:
            SQLScript: GenericSQLRelation

    clients:
        SQLScript: clients.get_db_client
        GenericSQLRelation: clients.get_metadata_client

    tasks:
        source: sql/create-table.sql
        name: some_task


Don't confuse the task's client with the product's client. **Task clients control
where to execute the code. Product clients control where to save metadata.**


Placeholders in SQL scripts
---------------------------

You can reference the ``product`` list in your ``pipeline.yaml`` in your script
using the ``{{product}}`` placeholder. For example ``[schema, name, table]``
renders to: ``schema.name``.

To specify upstream dependencies, use the ``{{upstream['some_task']}}``
placeholder. Here's a complete example:

.. code-block:: postgresql
    :class: text-editor
    :name: task-sql

    -- {{product}} gets replaced by the value in pipeline.yaml
    DROP TABLE IF EXISTS {{product}};

    CREATE TABLE {{product}} AS
    -- this task depends on the output generated by a task named "clean"
    SELECT * FROM {{upstream['clean']}}
    WHERE x > 10


Let's say our product is ``[schema, name, table]`` And the task named ``clean``
generates a product ``schema.clean``, the script above renders to:

.. code-block:: postgresql
    :class: text-editor
    :name: task-sql

    DROP TABLE IF EXISTS schema.name;

    CREATE TABLE schema.name AS
    SELECT * FROM schema.clean
    WHERE x > 10


If you want to see the rendered code for any task, execute the following in
the terminal:

.. code-block:: console

    ploomber task task_name --source

(Change ``task_name`` for the task you want)


**Note**: when executing a SQL script, you usually want to replace any existing
table/view. Some databases support the
``DROP TABLE IF EXISTS`` statement to do so, but other databases (e.g., Oracle)
have different procedures. Check your database's documentation for details.

**Important**: Some database drivers do not support sending multiple statements to the
database in a single call (e.g., SQLite), in such case, you can use the
``split_source`` parameter in either ``SQLAlchemyClient`` or ``DBAPIClient``
to split your statements and execute them one at a time, allowing you
to write a single ``.sql`` file to perform the
``DROP TABLE IF EXISTS`` then ``CREATE TABLE AS`` logic.


Mixing Python and SQL scripts via ``SQLDump``
---------------------------------------------

It's common to have pipelines where with some parts in SQL and others in
Python (e.g., preprocess the data in the database but train a model in Python).

To easily move data from your database to a local file, use the
:py:mod:`ploomber.tasks.SQLDump` task. Configuring this task is very similar
to a regular SQL task:

.. code-block:: yaml
    :class: text-editor
    :name: pipeline-yaml

    clients:
        # client for the database to pull data from
        SQLDump: clients.get_client

    tasks:
        # some sql tasks here...

        # dump the output of dump-query.sql
        source: sql/dump-query.sql
        # since this is a SQL dump, product is a path to a file
        product: output/data.csv

        # some python tasks here...

If you want to dump an entire table, you can do:

.. code-block:: postgresql
    :class: text-editor
    :name: dump-query.sql

    SELECT * FROM {{upstream['some_task']}}

Note that ``SQLDump`` only works with ``SQLAlchemyClient``. Product must be
a file with ``.csv`` or ``.parquet`` extension. The extension also helps
Ploomber that the given task is a ``SQLDump`` (instead of a ``SQLScript``).

By default, ``SQLDump`` downloads data in chunks of 10,000 rows. To dump
a single file: ``chunksize: null``, to choose another size:
``chunksize: n`` (e.g., ``chunksize: 1000000``).

**Important:** ``SQLDump`` uses ``pandas`` to dump data, which introduces
a considerable performance overhead. If you're dumping large tables, you may want
to implement a solution tailored to your database type.

Example pipeline
----------------

The following diagram shows our example pipeline along with some sample
source code for each task and the rendered version.

.. image:: https://ploomber.io/doc/sql/diag.png
   :target: https://ploomber.io/doc/sql/diag.png
   :alt: sql-diag

Other SQL tasks
---------------

There are other SQL tasks not covered here, check out the documentation for
details:

* :py:mod:`ploomber.tasks.SQLTransfer` (move data from one db to another)
* :py:mod:`ploomber.tasks.SQLDump` (upload data)
* :py:mod:`ploomber.tasks.PostgresCopyFrom` (efficient postgres data upload)


Where to go from here
---------------------

- :doc:`../user-guide/sql-templating` shows how to use jinja to write succinct SQL scripts
- `Advanced SQL pipeline example <https://github.com/ploomber/projects/tree/master/etl>`_
