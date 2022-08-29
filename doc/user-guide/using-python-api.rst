.. _Using-Python-In-Docs
Using the Python API in the docs
===============================

Unlike the command line interface, the Ploomber Python
API requires this preparation before each command:

.. code-block:: python
    :class: text-editor
    :name: task-py

    from ploomber.spec import DAGSpec
    dag = DAGSpec.find().to_dag()


For example, in building:

.. code-block:: python
    :class: text-editor
    :name: task-py

    # prepare
    from ploomber.spec import DAGSpec
    dag = DAGSpec.find().to_dag()

    # build
    dag.build()

is equivalent to:

.. code-block:: console

    ploomber build

For brevity the documentation does not include the prepare command in every code
snippet however it is necessary before every python command for proper
functionality
