Command line interface
======================

This document summarizes commonly used commands. To get full details, execute
``ploomber --help`` or ``ploomber {command_name} --help``.

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

The ``scaffold`` command allows you to start a new project. Note that this
creates a complete layout with packaging, version management, and testing.

.. code-block:: console

    ploomber scaffold


If you run this command in a folder that already has a ``pipeline.yaml`` in a
:ref:`api-cli-default-locations`, it will parse your pipeline declaration
looking for declared tasks whose source code file does not exist and proceed to
create them.

Interactive sessions
********************

To start an interactive session:

.. code-block:: console

    ploomber interact

Your pipeline is available in the  ``dag`` variable. Refer to
:py:mod:`ploomber.DAG` documentation for details.

Doing ``dag['task_name']`` returns a Task instance, all task instances have a
common API, but there are a few differences. Refer to the tasks documentation
for details: :ref:`tasks-list`.

The CLI guide describes some of the most common use cases for interactive
sessions: :ref:`user-guide-cli-interactive-sessions`.

Examples
********

To get a copy of the examples from the
`Github repository <https://github.com/ploomber/projects>`_.

List examples:

.. code-block:: console

    ploomber examples

Get one:

.. code-block:: console

    ploomber examples --name {name}


.. _api-cli-default-locations:

Default locations
*****************

Ploomber looks for a ``pipeline.yaml`` file using the following order: 

1. Use path in the ``ENTRY_POINT`` environment variable
2. ``pipeline.yaml``, relative to the current working directory
3. ``src/{package}/pipeline.yaml`` where ``{package}`` is any package name
4. Parent folders of current working directory
5. Look for a ``setup.py`` in parent folders, then ``src/{package}/pipeline.yaml``

If you want to know which file will be used based on your project's:

.. code-block:: console

    ploomber status --help

Look at the ``--entry-point`` description in the printed output.