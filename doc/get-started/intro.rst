
Introduction
============

Ploomber helps you manage your data analysis projects. Its design is based on a simple principle: *It is easier to understand (and solve) a problem when it is structured as small, isolated tasks.*

When working on a solo project, we might get away with a single big file that process all our data, but as soon as we want to work with someone else or share our work, structuring our project pays off.

**One task = one script**

Think of your data project as a series of ordered tasks (which we call "a pipeline"). A task can be a lot of things, but for the sake of this introduction it will be a Python script.

**Inputs and outputs**

Inputs for a task come from other "upstream" tasks. When a given task is finished, its outputs are consumed by its "downstream" tasks, this continues until we reach the final task.

**Standardizing tasks**

Since tasks need to talk to each other, having a standard interface helps stitch all the piece together without any extra work.

A simple pipeline
-----------------

Let's say we want to build a pipeline to generate a chart. We can divide the pipeline in three tasks: get raw data (\ ``raw.py``\ ), clean data (\ ``clean.py``\ ) and generate plot (\ ``plot.py``\ ). Our pipeline has the following order: ``raw -> clean -> plot``

Scripts as notebooks
--------------------


.. image:: https://ploomber.io/doc/script-and-notebook.png
   :target: https://ploomber.io/doc/script-and-notebook.png
   :alt: script-and-nb


A very popular format for developing Data Science projects is through Jupyter notebooks. Such format allows to store both code and rich output, while this is great for reviewing results, it's terrible for development because it complicates source code version control (i.e. git).

Ploomber follows an alternative approach: code your tasks as scripts but execute them as notebooks. This way you can keep a lean git workflow for development but still have the opportunity to embed rich output without extra work.

The fact that you use scripts for development does not mean you cannot develop them interactively, Ploomber relies on the great `jupytext <https://github.com/mwouts/jupytext>`_ package to seamlessly convert between scripts and notebooks when needed. This allows you to open your scripts just like notebooks.

Defining a standard interface
-----------------------------

We don't need to know any specifics about each script, as long as they respect the standard interface, we should be able to integrate them all in a pipeline.

In Ploomber, the task interface looks like this:

.. code-block:: python
    :class: text-editor
    :name: task-py

    # + tags=["parameters"]
    upstream = ['one_task', 'another_task']
    product = {'nb': 'path/to/task.ipynb', 'some_output': 'path/to/output.csv'}
    # -

The ``# +`` and ``# -`` markers define the *parameters cell* which is how we define our interface. Each task must contain this cell before any other logic (but after all imports).

The interface must define two variables:


#. ``upstream``\ : a list of task dependencies, the example above implies that the current task will use the outputs from ``one_task`` and ``another_task`` as inputs.
#. ``product``\ : a dictionary where whe specify output location. Keys are arbitrary names and values are paths.

As we mention in the previous section, all tasks are converted to notebooks before execution. Because of this, all scripts generate at least one file, to indicate where to save the executed notebook use the special ``nb`` key.

Cell injection
--------------


.. image:: https://ploomber.io/doc/injected-cell.png
   :target: https://ploomber.io/doc/injected-cell.png
   :alt: injected-cell


After converting your scripts to notebooks, there is one step left: cell injection.

When you declare ``upstream`` dependencies you only specify the upstream task name, but your code needs to know the exact file location to load it! While you could copy it from the upstream task source code, this leads to redundant logic which gets out of sync real quick.

Instead, s new cell is automatically injected by extracting the product from the upstream task, so you can use it as input.

To enable interactive development, cell injection also happens when you open your script in the Jupyter notebook app.

Defining a pipeline
-------------------

To perform all this logic, Ploomber needs to know which files to use as tasks of your pipeline, to do so, you only have to write a short ``pipeline.yaml`` file:

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


``name`` is optional, but it can be helpful when your tasks are spread in different folders.

Once you have a ``pipeline.yaml`` file, you can run your pipeline by executing the following command in the terminal:

.. code-block:: console

   ploomber entry pipeline.yaml

Ploomber keeps track of source changes to skip up-to-date tasks, if you run that command again, only tasks whose source code has changed will be executed.

Where to go from here
---------------------
