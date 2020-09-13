Integration with Jupyter
========================

Scripts go through a cell injection process that replaces the original upstream
variable (which only contains names for dependencies) with a dictionary that
maps these names to output files, so you can use it as inputs in the current
task.

For example if a Python script (``task.py``) declares the following dependency:

.. code-block:: python
    :class: text-editor
    :name: task-py

    upstream = ['another-task']


And ``another-task`` has the following product:

.. code-block:: python
    :class: text-editor
    :name: another-task-py

    product = {'nb': 'output/another-task.ipynb',
               'data': 'output/another-task.parquet'}


The following cell will be injected (below the original cell that defines
``upstream`` as a list) in ``task.py`` before execution:

.. code-block:: python
    :class: text-editor

    upstream = {'another_task': {'nb': 'output/another-task.ipynb',
                                 'data': 'output/another-task.parquet'}}

This means your code cannot be executed *as is*, because it needs the injected
cell for you to know the location of input files. To enable interactive
development, Ploomber integrates with the Jupyter notebook app.

If you open a script which a task in a pipeline, it will render as a
Jupyter notebook and the cell injection process will happen automatically
(this cell is just temporary and not saved). The Jupyter extension is
configured when you install Ploomber.

Activating the Jupyter extension
--------------------------------

In most cases, the extension is automatically enabled when you install
Ploomber, you can verify this by running:


.. code-block:: console

    jupyter serverextension list


If Ploomber appears in the list, it means it's activated. If it doesn't show
up, you can manually activate it with:

.. code-block:: console

    jupyter serverextension enable ploomber

To disable it:

.. code-block:: console

    jupyter serverextension disable ploomber


Pipeline loading
----------------

When you start the Jupyter app (via the ``jupyter notebook`` command), the
extension looks for a ``pipeline.yaml`` file in the current directory and
parent directories. If it finds one, it will load the pipeline and inject
the appropriate cell if the current file is task in the loaded pipeline.

If your are not using a ``pipeline.yaml`` file, but using just a directory with
scripts, you can override the default behavior by setting an ``ENTRY_POINT``
environment variable. For example, to load a pipeline from scripts in the
current directory:

.. code-block:: console

    export ENTRY_POINT=. && jupyter notebook



Troubleshooting pipeline loading
--------------------------------

If a pipeline is not detected, the Jupyter notebook application will just work
as expected, but no cell injection will happen. You can see if Ploomber could
not detect a pipeline by looking at the messages displayed after initializing
jupyter, you'll see something like this:

.. code-block:: console

    [Ploomber] No pipeline.yaml found, skipping DAG initialization...

If it is detected but fails to initialize, the Jupyter notebook will show an
error message in the terminal and then initialize:

.. code-block:: console

    [Ploomber] An error occurred when trying to initialize the pipeline.

Below such message, you'll see more details to help you debug your pipeline.


Detecting changes
-----------------

By default, pipelines are loaded when you start the Jupyter application, which
implies that upstream dependencies are defined at this point and don't change
even if you change them in your code (either the scripts themselves or the
``pipeline.yaml`` file). If you change dependencies, you have to restart the
Jupyter app.

You can enable hot reloading to make changes in dependencies refresh without
having to restart Jupyter, however, this is only supported if you're using
a ``pipeline.yaml`` file (not if your pipeline builds from a directory).

To enable this, set the ``jupyter_hot_reload`` (in the ``meta`` section) option
to ``True``. When this setting is enabled, the pipeline is loaded every time
you open a file, the time required to load a pipeline depends on the number
of tasks, for large pipelines, this might take a few seconds, hence, this option
is only recommended for small pipelines.