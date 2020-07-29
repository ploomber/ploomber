Command line interface
======================

This document summarizes commonly used commands, to get full details, execute
``ploomber --help`` or ``ploomber {command_name} --help``.

**Note:** All commands assume there is a ``pipeline.yaml`` file in the current
working directory, if your pipeline file is in a different location use the
``--entry_point/-e`` option.

Build pipeline
**************

.. code-block:: console

    ploomber entry

Building your pipeline means executing your pipeline end-to-end but speed it up
by skipping tasks whose source code has not changed.


Build pipeline partially
************************

.. code-block:: console

    ploomber entry --partially task_name


Builds your pipeline until it reaches task named ``task_name``.


Plot
****

.. code-block:: console

    ploomber plot


Will create a plot and save it in a ``pipeline.png`` file.

Report
******

.. code-block:: console

    ploomber report


Will create an HTML report and save it in a ``pipeline.html`` file. The file
includes the pipeline plot and a table with a summary for each task.


Build a single task
*******************

.. code-block:: console

    ploomber task task_name --build


Optionally add ``--force`` to force execution (ignore up-to-date status).


Get task status
***************

.. code-block:: console

    ploomber task task_name --status


Task source code
****************

.. code-block:: console

    ploomber task task_name --source


Create new project
******************

.. code-block:: console

    ploomber new


Interactive sessions
********************

Interactive sessions are a great way to develop your pipeline. Everything you
can do with the commands above, you can do it with an interactive session.

To start an interactive session:

.. code-block:: console

    ploomber interact

The command above starts a Python session, parses your pipeline and exposes it
in a ``dag`` variable, which is an instance of the :py:mod:`ploomber.DAG` class.

For example, to generate the plot:

.. code-block:: python
    :class: ipython

    dag.plot()

You can also interact with tasks, the specific API depends on which type of
task you are dealing with, see the :py:mod:`ploomber.tasks` documentation for
more information.

If you are working with Python scripts, you an start a line by line debugging
session:

.. code-block:: python
    :class: ipython

    dag['some_task'].debug()

To print the rendered source code from SQL scripts:

.. code-block:: python
    :class: ipython

    print(dag['some_sql_task'].source)


