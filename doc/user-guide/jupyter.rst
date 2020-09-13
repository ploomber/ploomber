Integration with Jupyter
========================

As described in the get started guide, scripts go through a cell injection
process that replaces the original upstream variable (which only contains
names for dependencies) with a dictionary that maps these names to output
files, so you can use it as inputs in the current task.

This means your code cannot be executed *as is*. To enable interactive
development, Ploomber integrates with the Jupyter notebook. If you open
a script which is a script in a pipeline, it will render as a Jupyter notebook
and the cell injection process will happen automatically (this cell is not
saved, though). The Jupyter extension is configured when you install Ploomber,
you don't have to setup anything.


Pipeline loading
----------------

When you start the Jupyter app (via the ``jupyter notebook`` command), the
extension looks for a ``pipeline.yaml`` file in the current directory and
parent directories. If it finds one, it will load the pipeline and inject
the appropriate cell if the file is task in the loaded pipeline.

If your are not using a ``pipeline.yaml`` file, but using just a directory with
scripts, you can override this behavior by setting the ``ENTRY_POINT``
environment variable. For example, to load a pipeline from the current
directory:

.. code-block:: console

    export ENTRY_POINT=. && jupyter notebook



Troubleshooting
---------------

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