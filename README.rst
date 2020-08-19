Ploomber
========

.. image:: https://travis-ci.org/ploomber/ploomber.svg?branch=master
    :target: https://travis-ci.org/ploomber/ploomber.svg?branch=master

.. image:: https://readthedocs.org/projects/ploomber/badge/?version=latest
    :target: https://ploomber.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://mybinder.org/badge_logo.svg
 :target: https://mybinder.org/v2/gh/ploomber/projects/master

.. image:: https://badge.fury.io/py/ploomber.svg
  :target: https://badge.fury.io/py/ploomber

.. image:: https://coveralls.io/repos/github/ploomber/ploomber/badge.svg?branch=master
  :target: https://coveralls.io/github/ploomber/ploomber?branch=master

Write better data pipelines without having to learn a specialized framework. By
adopting a convention over configuration philosophy, Ploomber streamlines
pipeline execution, allowing teams to confidently develop data products.

Installation
------------

.. code-block:: shell

    pip install ploomber


Compatible with Python 3.5 and higher.


Workflow
--------

Assume you have a collection of scripts, where each one is a task in your
pipeline.

To execute your pipeline end-to-end:

1. Inside each script, state dependencies (other scripts) via an ``upstream`` variable
2. Use a ``product`` variable to declare output file(s) that the next script will use as inputs
3. Run ``ploomber build --entry-point path/to/your/scripts/``

Optional: List your tasks in a ``pipeline.yaml`` file for more flexibility.

What you get
------------

1. Pipeline end-to-end execution
2. Incremental builds (skip up-to-date tasks)
3. Integration with Jupyter
4. Seamlessly integrate SQL with Python/R (i.e. extract data with SQL, plot it with Python/R)
5. `Parametrized pipelines with automatic command line interface generation <https://ploomber.readthedocs.io/en/stable/user-guide/parametrized.html>`_
6. `Templated SQL tasks (using jinja) <https://ploomber.readthedocs.io/en/stable/user-guide/sql-templating.html>`_
7. `Hooks for pipeline testing <https://ploomber.readthedocs.io/en/stable/user-guide/testing.html>`_
8. `Integration with debugging tools <https://ploomber.readthedocs.io/en/stable/user-guide/debugging.html>`_

How it looks like
-----------------

In Python scripts, declare your parameters like this:

.. code-block:: python

    # imports...

    # + tag=["parameters"]
    upsteam = ['some_task', 'another_task']
    product = {'nb': 'path/to/executed/nb.ipynb', 'data': 'path/to/data.csv'}
    # -

    # actual analysis code...

R scripts:

.. code-block:: R

    # imports...

    # + tag=["parameters"]
    upsteam = list('some_task', 'another_task')
    product = list(nb='path/to/executed/nb.ipynb', data='path/to/data.csv'}
    # -

    # actual analysis code...

Notebook (Python or R):

.. image:: https://ploomber.io/doc/ipynb-parameters-cell.png

SQL scripts:

.. code-block:: sql

    {% set product = SQLRelation(['schema', 'name', 'table']) %}

    DROP TABLE IF EXISTS {{product}};

    CREATE TABLE {{product}} AS
    SELECT FROM {{upstream['some_task']}}
    JOIN {{upstream['another_task']}}
    USING (some_column)


How it works
------------

1. Ploomber extracts dependencies from your code to infer execution order
2. Replaces the original ``upstream`` variable with one that maps tasks to their products (Python/R), see example below. Replaces placeholders with the actual table names (SQL)
3. Tasks are executed
4. Each script (Python/R) generates an executed notebook for you to review results visually

Example
-------

.. image:: https://ploomber.io/doc/python/diag.png


Demo
----

.. image:: https://asciinema.org/a/346484.svg
  :target: https://asciinema.org/a/346484


Try it out
----------

.. code-block:: shell

    ploomber new
    # follow instructions
    cd {project-name}
    ploomber build
    # see output in the output/ directory

**Note:** The demo project requires ``pandas`` and ``matplotlib``.

`Try out the hosted demo (no installation required) <https://mybinder.org/v2/gh/ploomber/projects/master?filepath=spec%2FREADME.md>`_.


External resources
------------------

* `Documentation <https://ploomber.readthedocs.io/>`_
* `Blog <https://ploomber.io/>`_



Python API
----------

There is also a Python API for advanced use cases. This API allows you build
flexible abstractions such as dynamic pipelines, where the exact number of
tasks is determined by its parameters. More information in the documentation.
