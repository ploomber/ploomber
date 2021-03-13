Jupyter and Exploratory Data Analysis
=====================================

Scripts go through a cell injection process that replaces the original upstream
variable (which only contains names for dependencies) with a dictionary that
maps these names to output files, so you can use them as inputs in the current
task.

For example if a Python script (``task.py``) declares the following dependency:

.. code-block:: python
    :class: text-editor
    :name: task-py

    upstream = ['another-task']


And ``another-task`` has the following ``product`` definition:

.. code-block:: yaml
    :class: text-editor

    tasks:
        - source: another-task.py
          product:
            nb: output/another-task.ipynb
            data: output/another-task.parquet


The following cell will be injected in ``task.py`` before execution:

.. code-block:: python
    :class: text-editor

    upstream = {'another_task': {'nb': 'output/another-task.ipynb',
                                 'data': 'output/another-task.parquet'}}

Without the injected cell, your code cannot execute because it doesn't know the input files' location. To enable interactive development, Ploomber integrates with the Jupyter (notebook and lab).

If you open a script that is a task in your pipeline, it will render as a
Jupyter notebook and the cell injection process will happen automatically
(this cell is just temporary and not saved). The Jupyter extension installs
when you install Ploomber.

**NOTE:** To view scripts as notebooks in Jupyter Lab: Right-click -> Open With -> Notebook.

Activating the Jupyter extension
--------------------------------

In most cases, the extension configures when you install Ploomber; you can verify this by running:


.. code-block:: console

    jupyter serverextension list


If Ploomber appears in the list, it means it's activated. If it doesn't show
up, you can manually activate it with:

.. code-block:: console

    jupyter serverextension enable ploomber

To disable it:

.. code-block:: console

    jupyter serverextension disable ploomber


Custom Jupyter pipeline loading
-------------------------------

When you start the Jupyter app (via the ``jupyter notebook`` command), the
extension looks for a ``pipeline.yaml`` file in the current directory and
parent directories. If it finds one, it will load the pipeline and inject
the appropriate cell if the existing file is a task in the loaded pipeline.

If you are not using a ``pipeline.yaml`` file, but using just a directory with
scripts, you can override the default behavior by setting an ``ENTRY_POINT``
environment variable. For example, to load a pipeline from scripts in the
current directory:

.. code-block:: console

    export ENTRY_POINT=. && jupyter notebook



Troubleshooting pipeline loading
--------------------------------

If a pipeline is not detected, the Jupyter notebook application will work
as expected, but no cell injection will happen. You can see if Ploomber could
not detect a pipeline by looking at the messages displayed after initializing Jupyter, you'll see something like this:

.. code-block:: console

    [Ploomber] No pipeline.yaml found, skipping DAG initialization...

If it is detected but fails to initialize, the Jupyter notebook will show an
error message in the terminal and then initialize:

.. code-block:: console

    [Ploomber] An error occurred when trying to initialize the pipeline.

Below such an error message, you'll see more details to help you debug your pipeline.


Detecting changes
-----------------

By default, pipelines load when you start the Jupyter application, which
implies that upstream dependencies are defined at this point and don't change
even if you change them in your code (either the scripts themselves or the
``pipeline.yaml`` file). If you change dependencies, you have to restart the
Jupyter app.

You can enable hot reloading to make changes in dependencies refresh without
having to restart Jupyter; however, this is only supported if you're using
a ``pipeline.yaml`` file (not if your pipeline builds from a directory).

To enable this, set the ``jupyter_hot_reload`` (in the ``meta`` section) option
to ``True``. When this setting is enabled, the pipeline is loaded every time
you open a file, the time required to load a pipeline depends on the number
of tasks, for large pipelines, this might take a few seconds; hence, this option
is only recommended for small pipelines.

Exploratory Data Analysis
-------------------------

There are two ways to use Ploomber in Jupyter. The first one, explained in
previous sections, is by loading a task file in Jupyter. However, this implies
that the file opened in Jupyter is a task in your pipeline.

A second way is to load your whole pipeline in Jupyter to interact with it.
Jupyter is an excellent approach during the initial stages since you don't know what's the best
to organize tasks or how many of them you need.

Say that you have a single task that loads the data:

.. code-block:: yaml
    :class: text-editor

    tasks:
        - source: load.py
          product:
            nb: output/load.ipynb
            data: output/data.csv


If you want to explore the raw data to decide how to organize downstream tasks (i.e., for data
cleaning). You can create a new notebook with the following code:

.. code-block:: python
    :class: text-editor
    :name: exploratory-ipynb

    from ploomber.spec import DAGSpec
    
    dag = DAGSpec.find().to_dag()


Note that this exploratory notebook **is not** part of your pipeline (i.e., it
doesn't appear in the ``tasks`` section of your ``pipeline.yaml``), but an
exploratory notebook that loads the pipeline.

The ``dag`` variable is an object that contains your pipeline definition. If you
want to load your raw data:

.. code-block:: python
    :class: text-editor

    import pandas as pd

    df = pd.read_csv(dag['load'].product)

Using the ``dag`` object avoids hardcoded paths to keep notebooks clean.

There are other things you can do with the ``dag`` object. See the following
guide for more examples: :ref:`user-guide-cli-interactive-sessions`.

As your pipeline grows, exploring it from Jupyter helps you decide what tasks to
build next and understand dependencies among tasks.

If you want to take a quick look at your pipeline, you may use
``ploomber interact`` from a terminal to get the ``dag`` object.
