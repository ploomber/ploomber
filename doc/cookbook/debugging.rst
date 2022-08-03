Debugging
=========

.. note:: This is a quick reference, for an in-depth tutorial, :doc:`click here <../user-guide/debugging>`.

.. note:: All this section assumes you're familiar with the Python debugger. See the `documentation here <https://docs.python.org/3/library/pdb.html>`_


.. _debugging-a-task:

Executing task in debugging mode
--------------------------------

To jump to the first line of a task and start a debugging session:

.. code-block:: console

    ploomber interact

Then:

.. code-block:: python
    :class: ipython

    dag['task-name'].debug()


.. note:: ``.debug()`` only works with Python functions, scripts, and notebooks.

To get the list of task names: ``list(dag)``.

After running ``.debug()``, you'll start a debugging session. You can use
the ``next`` command to jump to the next line. Type ``quit``, and hit enter
to exit the debugging session.


Post-mortem debugging
---------------------

Run and start a debugging session as soon as a task raises an exception.

.. code-block:: console

    ploomber task {task-name} --debug


.. code-block:: console

    ploomber build --debug


.. collapse:: changelog

    .. versionadded:: 0.20

        Added support for post-mortem debugging in notebooks using ``--debug``

    .. versionadded:: 0.20

        Added ``--debug`` option to ``ploomber task``


Post-mortem debugging (debug later)
-----------------------------------

*Added in version 0.20*

Run the pipeline and serialize errors from all failing tasks for later
debugging. This is useful when running tasks in parallel or notebooks
overnight.

.. code-block:: console

    ploomber task {task-name} --debuglater


.. code-block:: console

    ploomber build --debuglater


Then, to start a debugging session:

.. code-block:: console

    dltr {task-name}.dump

Once you're done debugging, you can delete the ``{task-name}.dump`` file.

.. collapse:: changelog

    .. versionadded:: 0.20

        Added ``--debuglater`` option to ``ploomber task``

    .. versionadded:: 0.20

        Added ``--debuglater`` option to ``ploomber build``


Breakpoints
-----------

.. note:: This only work with Python functions, go to the :ref:`next section <debugging-in-jupyter>` to learn how to debug scripts/notebooks.

Breakpoints allow you to start a debugger at given line:

.. code-block:: python
    :class: text-editor

    def my_task(product):
        # debugging session starts here...
        from ipdb import set_trace; set_trace()
        # code continues...


Then:

.. code-block:: console

    ploomber build --debug

.. _debugging-in-jupyter:

Debugging in Jupyter/VSCode
---------------------------

If you're using Jupyter or similar (e.g. notebooks in VSCode), you can debug there.

Post-portem
***********

If your code raises an exception, execute the following in a new cell, and a debugging session will start:


.. code-block:: python
    :class: text-editor

    %debug

`Click here <https://ipython.readthedocs.io/en/stable/interactive/magics.html#magic-debug>`_ to see the ``%pdb`` documentation.

If you want a debugging session to start whenever your code raises an exception:

.. code-block:: python
    :class: text-editor

    %pdb

.. note:: run ``%pdb`` again to turn it off.

`Click here <https://ipython.readthedocs.io/en/stable/interactive/magics.html#magic-pdb>`_ to see the ``%pdb`` documentation.

Breakpoints
***********

Once you're in Jupyter, you can add a breakpoint at the line you want to debug:

.. code-block:: python
    :class: text-editor

    def some_code_called_from_the_notebook():
        # debugging session starts here...
        from ipdb import set_trace; set_trace()
        # code continues...


The breakpoint can be in a module (i.e., something that you imported
using a ``import`` statement)


Visual debugger
***************

JupyterLab recently incorporated a native debugger, `click here <https://jupyterlab.readthedocs.io/en/stable/user/debugger.html>`_ to learn more.
