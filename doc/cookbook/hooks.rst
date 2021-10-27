Hooks (e.g., ``on_finish``)
===========================

Hooks allow you to execute an arbitrary function when a task finishes:

1. ``on_render`` executes right before executing the task.
2. ``on_finish`` executes when a task finishes successfully.
3. ``on_failure`` executes when a task errors during execution.

Suppose your ``pipeline.yaml`` looks like this:

.. code-block:: yaml
    :class: text-editor

    tasks:
        - source: tasks.my_task
          product: products/output.csv
          on_render: hooks.on_render
          on_finish: hooks.on_finish
          on_failure: hooks.on_failure


And your ``hooks.py`` file looks like this:

.. code-block:: python
    :class: text-editor

    def on_render():
        print('this runs before executing my_task')

    def on_finish():
        print('this runs when my_task executes without errors!')
    
    def on_failure():
        print('this runs when my_task raise an exception during execution!')


Hooks can take parameters; for example, you may add the ``product`` parameter
to the hook, and Ploomber will call the hook with the ``product`` for the
corresponding task. Adding arguments is useful when your hook needs information
from the task. Furthermore, you can pass arbitrary parameters loaded
from the ``pipeline.yaml``. To learn more about hook parameters
:ref:`click here <tasks-on-render-finish-failure>`.

Tip: Developing hooks interactively
-----------------------------------

If you want to develop hooks interactively, you may start a session inside your hook like this:

.. code-block:: python
    :class: text-editor

    def on_finish(product):
        from IPython import embed; embed()
        print('this runs when my_task executes without errors!')


Once you execute your pipeline, an interactive session will start. Interactive sessions are useful to explore the arguments (such as ``product``) that your hook can request.

If you want to exclusively run the ``on_finish`` hook (and skip task's execution):

.. code-block:: console

    ploomber task {task-name} --on-finish

Resources
---------

1. For a detailed look at the hooks API, :ref:`click here <tasks-on-render-finish-failure>`.
2. For a tutorial, check out :doc:`../user-guide/testing`, which is an in-depth guide of using ``on_finish`` for data testing.


DAG-level hooks
---------------

There are also DAG-level hooks, which work similarly. Declare them at the top section of your ``pipeline.yaml`` file:


.. code-block:: yaml
    :class: text-editor

    # dag-level hooks
    on_render: hooks.on_render
    on_finish: hooks.on_finish
    on_failure: hooks.on_failure

    tasks:
        - source: tasks.my_task
          product: products/output.csv

:ref:`Click here <on-render-finish-failure>` to learn more.