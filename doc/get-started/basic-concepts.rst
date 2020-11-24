
Basic concepts
==============

In the previous tutorial, we showed how to run a simple pipeline, this one
explains the concepts and design rationale.

Ploomber is based on a simple principle: *It is easier to understand (and
solve) a problem when it is structured as small, isolated tasks.* By adopting
a *convention over configuration* philosophy, Ploomber allows you to quickly
turn a collection scripts into a cohesive data pipeline by following three
simple **conventions**:

1. Each task is a Python function, (Python/R/SQL) script or Jupyter notebook
2. Scripts declare their dependencies in the ``upstream`` variable
3. Scripts declare their outputs in the ``product`` variable

A simple pipeline
-----------------

Let's say we want to build a pipeline to generate a chart. We can organize it
in three tasks: get raw data (\ ``raw.py``\ ), clean data (\ ``clean.py``\ )
and generate plot (\ ``plot.py``\ ):

.. raw:: html

    <div class="mermaid">
    graph LR
        raw.py --> clean.py --> plot.py
    </div>


Output(s) from one task become input(s) to "downstream" tasks. This means
"upstream" dependencies are interpreted in the opposite direction.
For example, in our pipeline, ``raw.py`` is an "upstream" dependency of
``clean.py``.

Scripts that execute as notebooks
---------------------------------

.. image:: https://ploomber.io/doc/script-and-notebook.png
   :target: https://ploomber.io/doc/script-and-notebook.png
   :alt: script-and-nb

A very popular format for developing Data Science projects is through Jupyter
notebooks. Such format allows to store both code and rich output. While this is
great for reviewing results, it makes source code version control harder
(i.e. git) because the ``.ipynb`` format embeds source code and output in the
same file.

Our recommended approach is to use plain scripts and use notebooks as an output
format, in the previous tutorial, we showed how Ploomber automatically
converts the source script to a notebook when you execute your pipeline.
Furthermore, thanks to the :doc:`../user-guide/jupyter`, you'll be able to
open scripts as notebooks and develop your pipeline interactively.

However, if you want to, you can still use the original ``.ipynb`` format as
the source code for your pipeline tasks.

``upstream`` dependencies and ``product``
-----------------------------------------

To state task dependencies, use an ``upstream`` variable. To declare outputs,
use a ``product`` variable. ``upstream`` must be a list with names of other
tasks and ``product`` a dictionary mapping keys to paths. Both variables must
be enclosed in special markup as follows:

.. code-block:: python
    :class: text-editor
    :name: task-py

    # + tags=["parameters"]
    upstream = ['one_task', 'another_task']
    product = {'nb': 'path/to/task.ipynb', 'some_output': 'path/to/output.csv'}
    # -

The ``# +`` and ``# -`` markers define a *cell*, while ``tags=["parameters"]``
tags it as the ``parameters`` cell.

A copy of of your script is generated prior execution, to indicate where you
want to save this copy, use the special ``nb`` key.

*Note:* We use `jupytext <https://github.com/mwouts/jupytext>`_ to convert scripts to
notebooks, see the documentation for formatting details.

Cell injection & Jupyter integration
------------------------------------

When you declare ``upstream`` dependencies you only specify the upstream task
name, but your code needs to know the exact file location to use it as input!
Ploomber automates this process.

When executing your pipeline, a new cell is automatically injected by
extracting the product from the upstream task.


.. image:: https://ploomber.io/doc/injected-cell.png
   :target: https://ploomber.io/doc/injected-cell.png
   :alt: injected-cell


As you can see in the image, the task has an upstream dependency called
``raw``, thus, the cell injected is a map that contains a ``raw`` key whose
value is the ``product`` declared for ``raw``.

Via a Jupyter plug-in, your scripts are rendered as notebooks. The cell
injection process happens as well, this enables interactive sessions that
exactly reproduce pipeline runtime conditions.

*Note:* Paths are converted to their absolute representations to avoid
ambiguity since pipeline configuration settings may change the working
directory at execution time.


Defining a pipeline
-------------------

To execute your pipeline, Ploomber needs to know which files to use as tasks,
do so in a ``pipeline.yaml`` file:

.. code-block:: yaml
    :class: text-editor
    :name: pipeline-yaml

    tasks:
      - source: raw.py
        name: raw

      - source: clean.py
        name: clean

      - source: plot.py
        name: plot


``name`` is optional, if not present, the value in ``source`` is used as task
identifier. This identifier is used to declare ``upstream`` dependencies.

Once you have a ``pipeline.yaml`` file, you can run your pipeline by executing
the following command:

.. code-block:: console

   ploomber build

Ploomber keeps track of source changes to skip up-to-date tasks, if you run
that command again, only tasks whose source code has changed will be executed.

For a full reference on ``pipeline.yaml`` files see: :doc:`../api/spec`

**Note:** Writing a ``pipeline.yaml`` file is optional, you can also create
pipelines by pointing to a directory with scripts. `Click here <https://github.com/ploomber/projects/tree/master/spec-api-directory>`_ to see an example.

**Note:** You can also define functions, instead of scripts, to be tasks in
your pipeline. Just add a dotted path in the ``source`` key, and make sure the
function can be imported. For example if you have a function that you can import
using ``from my_project.tasks import my_task``, add
``source: my_project.tasks.my_task`` to your ``pipeline.yaml`` file. See this
`example pipeline <https://github.com/ploomber/projects/tree/master/ml-basic>`_

Summary
-------

The following diagram shows our example pipeline along with some sample
source code for each task and the injected cell source code.


.. image:: https://ploomber.io/doc/python/diag.png
   :target: https://ploomber.io/doc/python/diag.png
   :alt: python-diag


Where to go from here
---------------------

Take a look at our `sample projects <https://github.com/ploomber/projects>`_
to see examples of common pipelines.

* `Basic Machine Learning pipeline <https://github.com/ploomber/projects/tree/master/ml-basic>`_
* `Intermediate ML pipeline <https://github.com/ploomber/projects/tree/master/ml-intermediate>`_ showing a few other features such as integration testing, parametrization and customization of output notebooks 
* `Advanced ML pipeline <https://github.com/ploomber/projects/tree/master/ml-advanced>`_ demonstrates a pipeline written using the Python API (as opposed to using a ``pipeline.yaml`` file, how to package your project, parallel execution and testing with ``pytest``
* The next tutorial (:doc:`../get-started/sql-pipeline`) shows how to build pipelines where some (or even all) the tasks as SQL scripts
