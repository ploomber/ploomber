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


To force the execution of tasks regardless of status, use the ``--force/-f`` option.

You can also select several tasks at the same time using wildcards:

.. code-block:: console

    ploomber build --partially 'fit-*' # note the single quotes


The previous command will execute all tasks with the ``fit-*`` prefix and all
their upstream dependencies.

You may skip building upstream dependencies using the ``--skip-upstream``

.. code-block:: console

    ploomber build --partially 'fit-*' --skip-upstream # note the single quotes

Note that the previous command fails if the upstream products of ``fit-*`` tasks
do not exist yet.

Plot
****

.. code-block:: console

    ploomber plot

Creates a pipeline plot and stores it.

**New in Ploomber 0.18.2**: You can plot the pipeline without installing extra dependencies.
``pygraphviz`` is still supported but optional. To learn more, :ref:`see this <faq-plotting-a-pipeline>`.


To include the task's products in the plot (only supported when using the ``pygraphviz`` backend):

.. code-block:: console

    ploomber plot --include-products


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


If you also want to build the task, you must explicitly pass ``--build``.

Task source code
****************

.. code-block:: console

    ploomber task task_name --source


If you also want to build the task, you must explicitly pass ``--build``.

.. _api-cli-ploomber-scaffold:

Create new project
******************

The ``scaffold`` command allows you to start a new project:

.. code-block:: console

    ploomber scaffold


The command above generates a project with sample pipeline. To create an empty
project:

.. code-block:: console

    ploomber scaffold --empty


**New in 0.16:** You can pass a positional argument ``ploomber scaffold myproject``.

Note that if you run this command in a folder that already has a
``pipeline.yaml`` in a :ref:`api-cli-default-locations`, it will parse your
pipeline declaration looking for declared tasks whose source code file does
not exist and proceed to create them.


.. code-block:: console

    ploomber scaffold


If you'd like to package your project:

.. code-block:: console

    ploomber scaffold --package


After creating a project, you can install dependencies
with the ``ploomber install`` command (to learn more: :ref:`api-cli-ploomber-install`).

For a tutorial on the ``ploomber scaffold`` command: :doc:`../user-guide/scaffold`.

.. _api-cli-ploomber-install:

``install``
***********

``ploomber install`` installs dependencies:

.. code-block:: console

    ploomber install


``ploomber install`` installs dependencies using ``pip`` if a
``requirements.txt`` file exists or  ``conda``, if
an ``environment.yml`` file exists.

**New in 0.16:** ``ploomber install`` has a few options to customize set up, run ``ploomber install --help`` to learn more.

**New in 0.16:** ``ploomber install`` will install dependencies in the current
environment, you can request creating a virtual environment with the ``--create-env`` option,
which will use `venv <https://docs.python.org/3/library/venv.html>`_ or ``conda`` (if installed). Previously, it always created a new environment

Upon installation, ``ploomber install`` generates lock files that contain
specific versions for all required packages. Lock files are useful for ensuring
the stability of your project since upgrades to your dependencies may break
your code. To install from lock files:

.. code-block:: console

    ploomber install --use-lock

**New in 0.16:** ``ploomber install`` (without arguments) will use lock files if they exist. Otherwise, it'll use regular files.

``nb``
******

``nb`` is short for *notebook*. This command manages notebooks and scripts in your pipeline.

Inject cell to scripts and notebooks in your pipeline:

.. code-block:: console

    ploomber nb --inject


Enable opening ``.py`` as notebooks in JupyterLab with one click on the file:

.. code-block:: console

    ploomber nb --single-click

Re-format ``.ipynb`` notebooks as ``.py`` files with the percent format:

.. code-block:: console

    ploomber nb -f py:percent


Re-format ``.py`` files as ``.ipynb`` notebooks:

.. code-block:: console

    ploomber nb -f ipynb


The rest of the options are useful when using editors such
as :doc:`VSCode or PyCharm <../user-guide/editors>` or when running old
JupyterLab versions (``<2.x``). When using recent JupyterLab versions,
script/notebooks management is automatically performed by
the :doc:`Jupyter plug-in <../user-guide/jupyter>`. 

Other commands are available, run ``ploomber nb --help`` to learn more.


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


To download in a specific location:


.. code-block:: console

    ploomber examples --name {name} --output path/to/dir


For a tutorial on the ``ploomber examples`` command: :doc:`../user-guide/templates`.


.. _api-cli-default-locations:

Default locations
*****************

If you don't pass the ``--entry-point/-e`` argument to the command line,
Ploomber will try to find one automatically by searching for a
``pipeline.yaml`` file at the current directory and parent directories.

If no such file exists, it looks for a ``setup.py``. If it exists, it searches
for a ``src/{pkg}/pipeline.yaml`` file where ``{pkg}`` is a folder with any
name. ``setup.py`` is only required for packaged projects.

If your pipeline has a different filename, you can create a ``setup.cfg`` file
and indicate what file you want to set as default. Note that **changing the
default affects both the command-line interface and the Jupyter plug-in**.

.. code-block:: cfg
    :class: text-editor
    :name: setup-cfg

    [ploomber]
    entry-point = path/to/pipeline.yaml

Note that paths are relative to the parent directory of ``setup.cfg``.

Alternatively, you may set the ``ENTRY_POINT``
environment variable to a different filename (e.g.,
``export ENTRY_POINT=pipeline.serve.yaml``). Note that this must be a filename,
not a path to a file.

If you want to know which file will be used based on your project's layout:

.. code-block:: console

    ploomber status --help

Look at the ``--entry-point`` description in the printed output.

.. collapse:: changelog

    .. versionadded:: 0.19.6
        Support for switching entry point with a ``setup.cfg`` file