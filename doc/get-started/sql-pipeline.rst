SQL pipelines
=============

This guide explains how to develop pipelines where some of all tasks are SQL scripts.

.. note::

    This tutorial shows the built-in SQL features. However, this is not
    the only way for Ploomber to interact with databases. You may as well create
    functions (or scripts) that run queries in a database. The primary benefit
    of using the built-in features is that Ploomber manages many things for you
    (such as active connections, running queries in parallel, dumping tables to
    local files), so you only write ``.sql`` files.

Quick Start
-----------

If you want to take a look at the sample pipeline, you have a few options:

- `Source code on Github <https://github.com/ploomber/projects/tree/master/templates/spec-api-sql>`_
- `Interactive demo <https://mybinder.org/v2/gh/ploomber/binder-env/main?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fploomber%252Fprojects%26urlpath%3Dlab%252Ftree%252Fprojects%252Ftemplates/spec-api-sql%252FREADME.ipynb%26branch%3Dmaster>`_

Or run it locally:


.. code-block:: console

    ploomber examples --name templates/spec-api-sql


Connecting to databases
-----------------------

.. note:: For a more detailed explanation on connecting to a database, see: :doc:`../cookbook/db-config`.

The first step to write a SQL pipeline is to tell Ploomber how to connect to
the database, by providing a function that returns either a
:py:mod:`ploomber.clients.SQLAlchemyClient` or a
:py:mod:`ploomber.clients.DBAPIClient`. These two clients cover all databases
supported by Python, even systems like Snowflake or Apache
Hive.

``SQLAlchemyClient`` takes a single argument, the database URI
(`Click here for documentation on sqlalchemy URIs <https://docs.sqlalchemy.org/en/13/core/engines.html>`_.). As the name
suggests, it uses SQLAlchemy under the hood, so any database supported by such
library is supported as well. Below, there's is an example that connects to
a local SQLite database:


.. code-block:: python
    :class: text-editor
    :name: clients-py

    from ploomber.clients import SQLAlchemyClient

    def get_client():
        return SQLAlchemyClient('sqlite:///database.db')


If SQLAlchemy doesn't support your database, you must use
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
        client: clients.get_client
        # task declaration continues...


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
        # all SQLDump tasks use the same client instance
        SQLDump: clients.get_client

    tasks:
        source: sql/create-table.sql
        # no need to add client here

``SQLScript`` (creates a table/view), and ``SQLDump`` (dump to a local file)
are the two most common types of SQL tasks, let's review them in detail.

Creating SQL tables/views with ``SQLScript``
--------------------------------------------

If you want to organize your SQL processing in multiple steps, you can use
``SQLScript`` to generate one table/view per task. The declaration in the
``pipeline.yaml`` file looks like this:

.. code-block:: yaml
    :class: text-editor

    tasks:
        source: sql/create-table.sql
        client: clients.get_client
        product: [schema, name, table]

``product`` can be a list with three elements: ``[schema, name, kind]``,
or 2: ``[name, kind]``. Where ``kind`` can be ``table`` or ``view``.

A typical script (``sql/create-table.sql`` in our case) looks like this:

.. code-block:: postgresql
    :class: text-editor
    :name: task-sql

    DROP TABLE IF EXISTS {{product}};

    CREATE TABLE {{product}} AS
    SELECT * FROM schema.clean
    # continues...

This ``DROP TABLE ... CREATE TABLE ..`` format ensures that the table
(or view) is deleted before creating a new version if the source code changes.

Note that we are using a ``{{product}}`` placeholder in our script, this will
be replaced at runtime for the name value in ``tasks[*].product`` (in our case:
``schema.name``.


``SQLScript`` and Product's metadata
-------------------------------------

Incremental builds (:ref:`incremental-builds`) allow you speed up pipeline
execution. To enable this, Ploomber keeps track of source code changes. When
tasks generate files (say ``data.csv``), a metadata file is saved next to
the product file (e.g., ``.data.csv.metadata``).

To enable incremental builds in ``SQLScript`` tasks, you must configure a
product metadata backend.

If you are using PostgreSQL, you can use
:py:mod:`ploomber.products.PostgresRelation`; if using SQLite, you can use
:py:mod:`ploomber.products.SQLiteRelation`. In both cases, metadata is saved
in the same database where the tables/views are created. Hence, you can reuse
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
:py:mod:`ploomber.products.GenericSQLRelation`, which stores metadata in a
SQLite database.

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
where to execute the code. Product clients manage where to save metadata.**


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


.. note::
    
    when executing a SQL script, you usually want to replace any existing
    table/view. Some databases support the
    ``DROP TABLE IF EXISTS`` statement to do so, but other databases (e.g., Oracle)
    have different procedures. Check your database's documentation for details.

.. important::
    
    Some database drivers do not support sending multiple statements to the
    database in a single call (e.g., SQLite), in such case, you can use the
    ``split_source`` parameter in either ``SQLAlchemyClient`` or ``DBAPIClient``
    to split your statements and execute them one at a time, allowing you
    to write a single ``.sql`` file to perform the
    ``DROP TABLE IF EXISTS`` then ``CREATE TABLE AS`` logic.


The following diagram shows our example pipeline along with some sample
source code for each task and the rendered version.

.. image:: https://ploomber.io/images/doc/sql/diag.png
   :target: https://ploomber.io/images/doc/sql/diag.png
   :alt: sql-diag

Dumping data with ``SQLDump``
-----------------------------

.. note:: ``SQLDump`` only works with :py:mod:`ploomber.clients.SQLAlchemyClient`.

A minimal SQLDump example is available `here <https://github.com/ploomber/projects/tree/master/cookbook/sql-dump>`_

If you want to dump the result of a SQL query, use
:py:mod:`ploomber.tasks.SQLDump`. Configuring this task is very similar to a
regular SQL task:

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
a file with ``.csv`` or ``.parquet`` extension.

By default, ``SQLDump`` downloads data in chunks of 10,000 rows, but yu can
change this value:

.. code-block:: yaml
    :class: text-editor

    tasks:
        source: sql/dump-query.sql
        product: output/data.csv
        # set chunksize to 1 million rows
        chunksize: 1000000

To dump a single file: ``chunksize: null``.

.. important::
    
    Downloading ``.parquet`` in chunks may yield errors if the schema inferred
    from one chunk is not the same as the one in another chunk. If you
    experience an issue, either change to ``.csv`` or set ``chunksize: null``.

.. important::
    
    ``SQLDump`` works with all databases supported by Python because it
    relies on ``pandas`` to dump data. However, this introduces a
    performance overhead. So if you're dumping large tables, consider
    implementing a solution optimized for your database.

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
- `Advanced SQL pipeline example <https://github.com/ploomber/projects/tree/master/templates/etl>`_
- `BigQuery example <https://github.com/ploomber/projects/tree/master/templates/google-cloud>`_
