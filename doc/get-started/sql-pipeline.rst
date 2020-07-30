SQL pipelines
=============

Connecting to databases
-----------------------

The first step to write a SQL pipeline is to tell Ploomber how to connect to
the database, this is done by providing a function that returns either a
:py:mod:`ploomber.clients.SQLAlchemyClient` or a
:py:mod:`ploomber.clients.DBAPIClient`. The former are a bit simpler to
configure, we'll cover such case for this example. These two clients cover
all databases supported by Python, even systems like Snowflake or Apache
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


Configuring the task client in ``pipeline.yaml``
------------------------------------------------

To configure your ``pipeline.yaml`` to run a SQL task, ``source`` and ``name``
are the same as for Python scripts. ``source`` must be a path to the SQL
scripts whereas ``name`` an identifier for the task.

To indicate how to load the client, you have to include the ``client`` key
under each task:

.. code-block:: yaml
    :class: text-editor
    :name: pipeline-yaml

    tasks:
        source: sql/load-data.sql
        name: some_task
        client: clients.get_client


The value in the ``client`` key must be a dotted path to the function that
loads your client. If your ``pipeline.yaml`` and ``clients.py`` are in the same
folder, you should be able to do this directly. If they are in a different
folder, you'll have to make sure that the function is importable.

You can reuse the same dotted path in several tasks in as many places as you
want, under the hood, Ploomber calls the function every time it encounters it
on your ``pipeline.yaml``.

Since it is common that most or all your SQL tasks perform operations in the
same database, you can also declare a task-level client like this:


.. code-block:: yaml
    :class: text-editor
    :name: pipeline-yaml

    clients:
        SQLScript: clients.get_client

    tasks:
        source: sql/load-data.sql
        name: some_task

This way you don't have to declare the same client on each task.

Product's metadata
------------------

In the previous tutorial, we showed that Ploomber lets you speed up pipeline
execution by skipping up-to-date tasks. It achieves so by saving metadata on
each product. For regular files, it creates a ``.source`` in the file's
location. For example if you generate a ``output/data.csv`` product, another
filed called ``output/data.csv.source`` is generated.

To support this for SQL relations (i.e. view or tables), Ploomber also has to
store metadata. If you are using PostgreSQL, you can use
:py:mod:`ploomber.products.PostgresRelation` and won't have to setup anything
else. If using SQLite, you can use :py:mod:`ploomber.products.SQLiteRelation`.

For any other database, you have two options, either use
:py:mod:`ploomber.products.SQLRelation` which is a product that does not save
any metadata at all (this means you won't have incremental runs) or use
:py:mod:`ploomber.products.GenericSQLRelation`, which stores metadata in a SQLite
database. SQLite is directly supported by Python, you don't have to install
nor setup anything else.

In all previously described cases, all SQL products rely on a database to
store metadata. This is why products also require a client. You can specify
a product's client like this:

.. code-block:: yaml
    :class: text-editor
    :name: pipeline-yaml

    tasks:
        source: sql/load-data.sql
        name: some_task
        # NOTE: client and product_client mean different things!
        product_client: clients.get_client

You can also declare product-level clients like this:

.. code-block:: yaml
    :class: text-editor
    :name: pipeline-yaml

    clients:
        PRODUCT_CLASS: clients.get_client

    tasks:
        source: sql/load-data.sql
        name: some_task


Where ``PRODUCT_CLASS`` is any of the valid SQL product classes:
``SQLiteRelation``, ``PostgresRelation``, ``GenericSQLRelation`` or
``SQLRelation``.

Don't confuse the task's client with the product's client. **Task clients control
where to execute the code, product clients control where to save metadata.**


Parametrized SQL scripts
------------------------

Similar to what we saw in the previous tutorial, each script contains an
``upstream`` and a ``product`` parameter that helps structure the pipeline. To
get this to work for SQL scripts we use the `jinja templating library <https://jinja.palletsprojects.com/en/2.11.x/>`_.

First, declare a ``product`` variable, which must be equal to any of the valid
SQL product classes, all of them take a list as its parameter. The first
element must be the schema, second one elation name and third one the kind
(view or table). If you want to use an implicit schema, pass a list with two
elements, for example: ``['name', 'table']``.

Since you have to reference the product in the SQL script, you can reference
to it using the ``{{product}}`` placeholder.

To specify upstream dependencies, use the ``{{upstream['some_task']}}``
placeholder. Let's see a complete example:

.. code-block:: postgresql
    :class: text-editor
    :name: task-sql

    -- this can be any of the valid product classes
    {% set product = SQLRelation(['schema', 'name', 'table']) %}

    -- {{product}} gets replaced by the variable defined above
    DROP TABLE IF EXISTS {{product}};

    CREATE TABLE {{product}} AS
    -- this task depends on the output generated by a task named "clean"
    SELECT * FROM {{upstream['clean']}}
    WHERE x > 10


Let's say there is task named ``clean`` that generates a product
``schema.clean``, the script above renders to the following:

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
table/view with the same name. Some databases support the
``DROP TABLE IF EXISTS`` statement to do so, but other databases (e.g. Oracle)
have different procedures. Check your database's documentation for details.

**Important**: Some database drivers do not support sending multiple statements to the
database in a single call (e.g. SQLite), in such case, you can use the
``split_source`` parameter in either ``SQLAlchemyClient`` or ``DBAPIClient``
to split your statements and execute them one at a time. This allows you
to write a single ``.sql`` file to perform the
``DROP TABLE IF EXISTS ... CREATE TABLE AS ...`` logic.


Mixing Python and SQL scripts via ``SQLDump``
---------------------------------------------

It's common to have pipelines where parts are written in SQL and parts in
Python (e.g. preprocess the data in the database but train a model in Python).

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

        # indicate this is a SQLDump task
        class: SQLDump
        source: sql/dump-query.sql
        name: some_task

        # some python tasks here...

``SQLDump`` also has a ``source`` parameter, which allows you to optionally
filter the data to dump. If you want to dump an entire table you can just do:

.. code-block:: postgresql
    :class: text-editor
    :name: dump-query.sql

    SELECT * FROM {{upstream['some_task']}}

Note that ``SQLDump`` only works with
``SQLAlchemyClient``, it is designed to be flexible, but it comes with some
performance considerations. Review the task's documentation for details.


Wrapping up
-----------

This tutorial introduced several new ideas. It might be hard to wrap your head
around all these concepts, to make things clearer, feel free to go to our
interactive example, which implements a pipeline with some SQL tasks, a
``SQLDump`` and a Python task:

.. image:: https://img.shields.io/badge/launch-session-579ACA.svg?logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAFkAAABZCAMAAABi1XidAAAB8lBMVEX///9XmsrmZYH1olJXmsr1olJXmsrmZYH1olJXmsr1olJXmsrmZYH1olL1olJXmsr1olJXmsrmZYH1olL1olJXmsrmZYH1olJXmsr1olL1olJXmsrmZYH1olL1olJXmsrmZYH1olL1olL0nFf1olJXmsrmZYH1olJXmsq8dZb1olJXmsrmZYH1olJXmspXmspXmsr1olL1olJXmsrmZYH1olJXmsr1olL1olJXmsrmZYH1olL1olLeaIVXmsrmZYH1olL1olL1olJXmsrmZYH1olLna31Xmsr1olJXmsr1olJXmsrmZYH1olLqoVr1olJXmsr1olJXmsrmZYH1olL1olKkfaPobXvviGabgadXmsqThKuofKHmZ4Dobnr1olJXmsr1olJXmspXmsr1olJXmsrfZ4TuhWn1olL1olJXmsqBi7X1olJXmspZmslbmMhbmsdemsVfl8ZgmsNim8Jpk8F0m7R4m7F5nLB6jbh7jbiDirOEibOGnKaMhq+PnaCVg6qWg6qegKaff6WhnpKofKGtnomxeZy3noG6dZi+n3vCcpPDcpPGn3bLb4/Mb47UbIrVa4rYoGjdaIbeaIXhoWHmZYHobXvpcHjqdHXreHLroVrsfG/uhGnuh2bwj2Hxk17yl1vzmljzm1j0nlX1olL3AJXWAAAAbXRSTlMAEBAQHx8gICAuLjAwMDw9PUBAQEpQUFBXV1hgYGBkcHBwcXl8gICAgoiIkJCQlJicnJ2goKCmqK+wsLC4usDAwMjP0NDQ1NbW3Nzg4ODi5+3v8PDw8/T09PX29vb39/f5+fr7+/z8/Pz9/v7+zczCxgAABC5JREFUeAHN1ul3k0UUBvCb1CTVpmpaitAGSLSpSuKCLWpbTKNJFGlcSMAFF63iUmRccNG6gLbuxkXU66JAUef/9LSpmXnyLr3T5AO/rzl5zj137p136BISy44fKJXuGN/d19PUfYeO67Znqtf2KH33Id1psXoFdW30sPZ1sMvs2D060AHqws4FHeJojLZqnw53cmfvg+XR8mC0OEjuxrXEkX5ydeVJLVIlV0e10PXk5k7dYeHu7Cj1j+49uKg7uLU61tGLw1lq27ugQYlclHC4bgv7VQ+TAyj5Zc/UjsPvs1sd5cWryWObtvWT2EPa4rtnWW3JkpjggEpbOsPr7F7EyNewtpBIslA7p43HCsnwooXTEc3UmPmCNn5lrqTJxy6nRmcavGZVt/3Da2pD5NHvsOHJCrdc1G2r3DITpU7yic7w/7Rxnjc0kt5GC4djiv2Sz3Fb2iEZg41/ddsFDoyuYrIkmFehz0HR2thPgQqMyQYb2OtB0WxsZ3BeG3+wpRb1vzl2UYBog8FfGhttFKjtAclnZYrRo9ryG9uG/FZQU4AEg8ZE9LjGMzTmqKXPLnlWVnIlQQTvxJf8ip7VgjZjyVPrjw1te5otM7RmP7xm+sK2Gv9I8Gi++BRbEkR9EBw8zRUcKxwp73xkaLiqQb+kGduJTNHG72zcW9LoJgqQxpP3/Tj//c3yB0tqzaml05/+orHLksVO+95kX7/7qgJvnjlrfr2Ggsyx0eoy9uPzN5SPd86aXggOsEKW2Prz7du3VID3/tzs/sSRs2w7ovVHKtjrX2pd7ZMlTxAYfBAL9jiDwfLkq55Tm7ifhMlTGPyCAs7RFRhn47JnlcB9RM5T97ASuZXIcVNuUDIndpDbdsfrqsOppeXl5Y+XVKdjFCTh+zGaVuj0d9zy05PPK3QzBamxdwtTCrzyg/2Rvf2EstUjordGwa/kx9mSJLr8mLLtCW8HHGJc2R5hS219IiF6PnTusOqcMl57gm0Z8kanKMAQg0qSyuZfn7zItsbGyO9QlnxY0eCuD1XL2ys/MsrQhltE7Ug0uFOzufJFE2PxBo/YAx8XPPdDwWN0MrDRYIZF0mSMKCNHgaIVFoBbNoLJ7tEQDKxGF0kcLQimojCZopv0OkNOyWCCg9XMVAi7ARJzQdM2QUh0gmBozjc3Skg6dSBRqDGYSUOu66Zg+I2fNZs/M3/f/Grl/XnyF1Gw3VKCez0PN5IUfFLqvgUN4C0qNqYs5YhPL+aVZYDE4IpUk57oSFnJm4FyCqqOE0jhY2SMyLFoo56zyo6becOS5UVDdj7Vih0zp+tcMhwRpBeLyqtIjlJKAIZSbI8SGSF3k0pA3mR5tHuwPFoa7N7reoq2bqCsAk1HqCu5uvI1n6JuRXI+S1Mco54YmYTwcn6Aeic+kssXi8XpXC4V3t7/ADuTNKaQJdScAAAAAElFTkSuQmCC
   :target: https://mybinder.org/v2/gh/ploomber/projects/master?filepath=spec%2FREADME.md

Once you start developing SQL pipelines this way, you'll realize how much
faster it is than the old way. Instead of managing database connections,
making sure you read from the right tables, checking if dependencies are up-to-date
or writing custom code to dump data from a database, you can focus on writing
the SQL and let Ploomber take care of the rest.


Where to go next
----------------

Through these initial three tutorials, you learned the fundamental concepts.
This will help you start using Ploomber in your data projects. There are other
important features that can help you improve your development workflow, check
out the :doc:`../user-guide/index` to learn more advanced techniques.