Basic concepts
==============

In the previous tutorial, we showed how to run a simple pipeline, this guide
explains Ploomber's core concepts in detail and the overall design rationale.

Ploomber is based on a simple principle: It is easier to understand (and
solve) a problem when structured as small, isolated tasks. By adopting
a *convention over configuration* philosophy, Ploomber allows you to quickly
turn a collection functions, scripts, or notebooks into a data pipeline by
following three simple conventions:

1. Each task is a Python function, (Python/R/SQL) script or Jupyter notebook
2. Scripts declare their dependencies using an ``upstream`` variable
3. Scripts declare their outputs using a ``product`` variable


A simple pipeline
-----------------

Let's say we want to build a pipeline to plot some data. We can organize it
in three tasks: get raw data, clean it, and generate a plot:

.. raw:: html

    <div class="mermaid">
    graph LR
        raw --> clean --> plot
    </div>

Given its structure, a pipeline is also referred as a directed acyclic graph
(or DAG), we use both terms interchangeably.

In a Ploomber pipeline, output(s) (also known as **products**) from one
task become input(s) to "downstream" tasks. "upstream" dependencies read in
the opposite direction. For example, ``raw`` is an "upstream" dependency of ``clean``.

An "upstream" dependency implies that a given task uses its upstream
dependencies products as inputs. Following our pipeline example,
``clean`` uses ``raw``'s product, and ``plot`` uses ``clean``'s product.

Ploomber supports three types of tasks:

1. Python functions (also known as callables)
2. Python/R scripts/notebooks
3. SQL scripts

You can develop pipelines where all tasks are functions, scripts, notebooks,
SQL scripts, or any combination of them. They all have the same interface, but
details vary. We describe the nuances in upcoming sections.

Sections are independent; you can skip to whatever task type you want to know
more.

Defining a pipeline
-------------------

To execute your pipeline, Ploomber needs to know where the task's source code
is and what the products are. This is done via a ``pipeline.yaml`` file:

.. code-block:: yaml
    :class: text-editor
    :name: pipeline-yaml

    tasks:
      # sql script task
      - source: raw.sql
        product: [schema, name, table]
        # task definition continues...

      # function task (equivalent to: from my_functions import clean)
      - source: my_functions.clean
        product: output/clean.csv

      # script task (notebooks work the same)
      - source: plot.py
        product:
          # generates a notebook (more on this in the next section)
          nb: output/plots.ipynb


You can set a specific name using ``name``. If not present, it is inferred from
the ``source`` value.

Once you have a ``pipeline.yaml`` file, run it with:

.. code-block:: console

   ploomber build

Ploomber keeps track of source changes to skip up-to-date tasks if you run
that command again, only tasks whose source code has changed are executed.

For a full reference on ``pipeline.yaml`` files see: :doc:`../api/spec`

**Note:** Writing a ``pipeline.yaml`` file is optional; you can also create
pipelines by pointing to a directory with scripts. `Click here <https://github.com/ploomber/projects/tree/master/spec-api-directory>`_
to see an example. However, this is only recommended for simple projects
(i.e., pipelines with just a couple tasks).


Tasks: scripts/notebooks
------------------------

The Jupyter notebook format (``.ipynb``) is prevalent for developing Data
Science projects. One of its main features is code storage and output in a
standalone file. While this is great for exploratory analysis, it
makes code version control harder (i.e., it isn't trivial to get the
code diff between version A and B).

Our recommended approach is to use scripts but to keep the benefits of the
``.ipynb`` format, Ploomber creates a copy of your scripts and converts it to
``.ipynb`` at runtime. *This is a key concept: scripts are part of your
project's source code, but output notebooks are not. They're pipeline
products*. The following image shows a side-by-side comparison:

.. image:: https://ploomber.io/doc/script-and-notebook.png
   :target: https://ploomber.io/doc/script-and-notebook.png
   :alt: script-and-nb

Note the special ``# +`` and ``# -`` markers in the script; they delimit
notebook cells. Other formats are supported, refer
to `jupytext <https://github.com/mwouts/jupytext>`_ documentation for details.

Thanks to the :doc:`../user-guide/jupyter`, you can open scripts in Jupyter, as
if they were ``.ipynb`` files. You can still use regular ``.ipynb``
files as tasks (but a copy is still created when you execute the pipeline).

R scripts are supported as well. See this: :doc:`../user-guide/r-support`.

``upstream`` and ``product``
****************************

To specify dependencies, include a special ``parameters`` cell in your
script/notebook. Following our example pipeline, ``plot`` has ``clean``
as an upstream dependency, we establish this by declaring an ``upstream``
variable in the special ``parameters`` cell:

.. code-block:: python
    :class: text-editor
    :name: plot-py

    # + tags=["parameters"]
    upstream = ['plot']
    # -

Note we tagged the cell using ``tags=["parameters"]``. If the notebook doesn't
have dependencies, set ``upstream = None``.

**Note:** the ``# +`` and ``# -`` markers only apply to scripts.
`Click here <https://papermill.readthedocs.io/en/stable/usage-parameterize.html>`_
for information on adding tags to ``.ipynb`` files.

The previous code won't run as it is. It contains the list of upstream tasks,
but we don't know the products generated by them. Furthermore, we don't know
where to same the output of this task.

During execution, Ploomber injects a new cell, with ``product`` and
``upstream`` variables.

.. image:: https://ploomber.io/doc/injected-cell.png
   :target: https://ploomber.io/doc/injected-cell.png
   :alt: injected-cell


As you can see in the image, the task in the picture has an upstream
dependency called ``raw``. Thus, the cell injected is a map that contains the
product location of ``raw``, which we use as input. Furthermore, whatever
value we have in the ``product`` key is passed; we use that variable to save
the current task's output.

**Note:** Cell injection also happens when opening the file in Jupyter notebook/lab.

Since scripts/notebooks always create an executed notebook, you must specify
where to save such file, a typical task declaration looks like this:

.. code-block:: yaml
    :class: text-editor

    tasks:
      - source: plot.py
        # output notebook
        product: output/plots.ipynb

.. code-block:: yaml
    :class: text-editor

    tasks:
      - source: plot.py
        product:
          # if the script generates other products, use "nb" for the notebok
          nb: output/plots.ipynb
          # ...and any other keys for other files
          data: output/data.csv


Examples
********

1. `Click here <https://github.com/ploomber/projects/tree/master/ml-basic>`_ to see an example pipeline that contains a script-based task that trains a model.


Tasks: functions
----------------

You can also use functions as tasks.

``upstream`` and ``product``
****************************

The only requirement for the function is to have a ``product`` parameter.

.. code-block:: python
   :class: text-editor
   :name: my_functions-py

   import pandas as pd

   def clean(product):
      # your code here...
      # save output using the product argument, e.g.,
      df.to_csv(product)


If the task has upstream dependencies, add an ``upstream`` parameter:

.. code-block:: python
   :class: text-editor

   import pandas as pd

   def clean(product, upstream):
      df_input = pd.read_csv(upstream['task_name'])
      df.to_csv(product)

When resolving dependencies, Ploomber will look for references such as
``upstream['task_name']``. At runtime, the function executed with:
``upstream={'task_name': 'path/to/product/from/upstream.csv'}``

Examples
********

1. `Click here <https://github.com/ploomber/projects/tree/master/ml-basic>`_ to see an example pipeline that includes some function-based tasks to generate features and then trains a model.
2. `Click here <https://github.com/ploomber/projects/tree/master/ml-intermediate>`_ to see a more elaborate ML pipeline example, which shows how to create a training and batch serving pipeline.
3. `Click here <https://github.com/ploomber/projects/tree/master/ml-online>`_ to see our most complete example: an end-to-end ML pipeline that can be trained locally, in Kubernetes or Airflow and can be deployed as a microservice using Flask.

Tasks: SQL
----------

SQL tasks require more setup because you have to configure a ``client`` to
connect to the database. We explain the ``product`` and ``upstream`` mechanism
here; the following guide describes how clients work.

``upstream`` and ``product``
****************************

SQL scripts require placeholders for ``product`` and ``upstream``. A script
that has no upstream dependencies looks like this:

.. code-block:: postgresql
   :class: text-editor
   :name: raw-sql

   -- {{product}} is a placeholder
   CREATE TABLE {{product}} AS
   SELECT * FROM my_table WHERE my_column > 10

In your ``pipeline.yaml`` file, specify ``product`` with a list of 3
or 2 elements: ``[schema, name, table]`` or ``[name, table]``. If using a
view, use ``[schema, name, view]``

Say you have ``product: [schema, name, table]`` in your ``pipeline.yaml`` file.
The script above renders to:

.. code-block:: postgresql
   :class: text-editor
   :name: raw-sql

   CREATE TABLE schema.name AS
   SELECT * FROM my_table WHERE my_column > 10

If the script has upstream dependencies, use the ``{{upstream['task_name']}}``
placeholder:

.. code-block:: postgresql
   :class: text-editor
   :name: raw-sql

   CREATE TABLE {{product}} AS
   SELECT * FROM {{upstream['task_name']}} WHERE my_column > 10

``{{upstream['task_name']}}`` tells Ploomber to run the task with the name
``'task_name'`` first and to replace ``{{upstream['task_name']}}`` with the
product of such task.

Clients
*******

To establish a connection with a database, you have to configure a ``client``.
All databases that have a Python driver are supported, including systems like
Snowflake or Apache Hive. For details see :doc:`../api/spec`.

Examples
********

1. `Click here <https://github.com/ploomber/projects/tree/master/spec-api-sql>`_ to see an example pipeline that processes data in a database, dumps it, and generates some charts with Python.
2. `Click here <https://github.com/ploomber/projects/tree/master/etl>`_ to see a pipeline that downloads data, uploads it to a database, process it, dumps it, and generates charts with Python.

Using the Python API
--------------------

The ``pipeline.yaml`` API offers a concise and powerful way to declare
pipelines, but if you want complete flexibility, you can use the underlying Python
API directly, `here's a basic example <https://github.com/ploomber/projects/tree/master/python-api>`_.
And here's a more `ellaborated Machine Learning example <https://github.com/ploomber/projects/tree/master/ml-advanced>`_.


Where to go from here
---------------------

This guide covered Ploomber's core concepts. You are ready to create
pipelines! If you want to learn what other features there are, check out the
API documentation: :doc:`../api/spec`.

If you want to learn how to build pipelines that interact with SQL database, go
to the next tutorial: :doc:`../get-started/sql-pipeline`.
