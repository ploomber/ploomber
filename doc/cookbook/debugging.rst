Debugging
=========

*This is a quick reference, for an in-depth tutorial,* :doc:`click here <../user-guide/debugging>`.

Ploomber has some built-in debugging features, and it integrates with the Python `debugger <https://docs.python.org/3/library/pdb.html>`_

.. _debugging-a-task:

Debugging a task
----------------

**Note:** ``.debug()`` works with Python functions, scripts, and notebooks.

Start an interactive session:

.. code-block:: console

    ploomber interact

Start a debugging session:

.. code-block:: python
    :class: ipython

    dag[task_name].debug()


To get the list of task names: ``list(dag)``.

After running ``.debug()``, you'll start a debugging session, type ``quit`` and hit enter to exit the debugging session.

**Note:** If debugging a script or notebook, you may call ``.debug(kind='pm')`` to start a post-mortem session, which runs the script/notebook until the code raises an exception, then the debugging session begins.

Post-mortem debugging
---------------------

**Note:** ``--debug`` only works with Python functions,  go to the :ref:`next section <debugging-in-jupyter>` to learn how to debug scripts/notebooks.

If you want the pipeline to run and start the debugging session the momeent it raises an exception:


.. code-block:: console

    ploomber build --debug


Breakpoints
-----------

**Note:** This only work with Python functions, go to the :ref:`next section <debugging-in-jupyter>` to learn how to debug scripts/notebooks.

If you want to stop execution at an arbitrary line:

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

Debugging in Jupyter
--------------------

If you want to debug scripts or notebook, you can do so from
the :ref:`terminal <debugging-a-task>`, or from Jupyter. If you want to use
Jupyter, first, open the script/notebook.

Post-portem
***********

If your code raises an exception, execute the following in a new cell and a debugging session will start:


.. code-block:: python
    :class: text-editor

    %debug

`Click here <https://ipython.readthedocs.io/en/stable/interactive/magics.html#magic-debug>`_ to see the ``%pdb`` documentation.

If you want a debugging session to start whenever your code raises an exception:

.. code-block:: python
    :class: text-editor

    %pdb

**Note:** run ``%pdb`` again to turn it off.

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


The breakpoint can be in an a module (i.e., something that you imported
using a ``import`` statement)


Visual debugger
***************

JupyterLab recently incorporated a native debugger, `click here <https://jupyterlab.readthedocs.io/en/stable/user/debugger.html>`_ to learn more.

Using the debugger
------------------

Once you enter a debugging session, there are a few comands you can run. For example, you can execute ``quit`` to exit the debugger. For a complete list of available commands see the `documentation <https://docs.python.org/3/library/pdb.html>`_