Command line interface
======================

This document summarizes commonly used commands. To get full details, execute
``ploomber --help`` or ``ploomber {command_name} --help``.

**Note:** All commands assume there is a ``pipeline.yaml`` file in the current
working directory, if your pipeline file is in a different location use the
``--entry-point/-e`` option.

When applicable, we use this sample pipeline to demonstrate which tasks will
be executed after issuing a given command:

.. raw:: html

    <div class="mermaid">
    graph LR
        A --> B1 --> C --> D
        A --> B2 --> C

        class A outdated
        class B1 outdated
        class B2 uptodate
        class C outdated
        class D outdated
    </div>

Assume yellow tasks are outdated and green tasks are up-to-date.

Executed tasks are shown in blue and skipped tasks are shown in white in
diagrams below.

Build pipeline
**************

.. code-block:: console

    ploomber build


Execute your pipeline end-to-end and speed it up by skipping tasks whose
source code has not changed.

.. raw:: html

    <div class="mermaid">
    graph LR
        A --> B1 --> C --> D
        A --> B2 --> C

        class A executed
        class B1 executed
        class B2 skipped
        class C executed
        class D executed
    </div>

(Skips ``B2`` because it's up-to-date)


Build pipeline (forced)
***********************

.. code-block:: console

    ploomber build --force


Execute all tasks regardless of status.

.. raw:: html

    <div class="mermaid">
    graph LR
        A --> B1 --> C --> D
        A --> B2 --> C

        class A executed
        class B1 executed
        class B2 executed
        class C executed
        class D executed
    </div>

Build pipeline partially
************************


.. code-block:: console

    ploomber build --partially C


Builds your pipeline until it reaches task named ``C``.

.. raw:: html

    <div class="mermaid">
    graph LR
        A --> B1 --> C --> D
        A --> B2 --> C

        class A executed
        class B1 executed
        class B2 skipped
        class C executed
        class D skipped
    </div>


(Skips ``B2`` because it's up-to-date)

(Skips ``D`` because it's not needed to build ``C``)


To force execution of tasks regardless of status use the ``--force/-f`` option.

Plot
****

.. code-block:: console

    ploomber plot


Create a pipeline plot and save it in a ``pipeline.png`` file.

Status
******

.. code-block:: console

    ploomber status


Show a table with pipeline status. For each task: name, last execution time,
status, product, docstring (first line) and file location.

Report
******

.. code-block:: console

    ploomber report


Create an HTML report and save it in a ``pipeline.html`` file. The file
includes the pipeline plot and a table with a summary for each task.


Build a single task
*******************

.. raw:: html

    <div class="mermaid">
    graph LR
        A --> B1 --> C --> D
        A --> B2 --> C

        class A skipped
        class B1 skipped
        class B2 skipped
        class C executed
        class D skipped
    </div>

.. code-block:: console

    ploomber task C


To force execution regardless of status use the ``--force/-f`` option.

Get task status
***************

.. code-block:: console

    ploomber task task_name --status


If you also want to build the task, you need to explicitly pass ``--build``.

Task source code
****************

.. code-block:: console

    ploomber task task_name --source


If you also want to build the task, you need to explicitly pass ``--build``.

Create new project
******************

.. code-block:: console

    ploomber new


Interactive sessions
********************

Interactive sessions are a great way to develop your pipeline. Everything you
can do with the commands above (and more), you can do it with an interactive
session.

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

If you are working with Python scripts, you can start a line by line debugging
session:

.. code-block:: python
    :class: ipython

    dag['some_task'].debug()

To print the rendered source code from SQL scripts:

.. code-block:: python
    :class: ipython

    print(dag['some_sql_task'].source)


