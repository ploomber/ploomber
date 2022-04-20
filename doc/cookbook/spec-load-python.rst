Custom pipeline loading logic
=============================

.. note::

    For a complete code example `click here <https://github.com/ploomber/projects/tree/master/cookbook/python-load>`_

When writing a pipeline via a ``pipeline.yaml`` file, the most common way to
interact is via the command-line interface (e.g., calling ``ploomber build``).
However, we may want to add more logic or embed it as part of a script in some cases.

Ploomber represents pipelines using ``DAG`` objects. You can convert your
``pipeline.yaml`` to a ``DAG`` object in Python with the following code:

.. code-block:: python
    :class: text-editor
    :name: pipeline-py

    from ploomber.spec import DAGSpec

    def load():
        spec = DAGSpec('pipeline.yaml')
        dag = spec.to_dag()
        return dag


The ``spec`` variable is an object of type :class:`ploomber.spec.DAGSpec`, while
``dag`` is of type :class:`ploomber.DAG`, to learn more about their interfaces,
click on any of the links.

Assume the code above exists in a file named ``pipeline.py``; you may load
the pipeline from the CLI with:

.. code-block:: console

    ploomber build --entry-point pipeline.load


Or the shortcut:

.. code-block:: console

    ploomber build -e pipeline.load


The ``load`` function may contain extra logic; for example, you may skip tasks
based on some custom rules or compute dynamic parameters. For examples with
custom logic, `click here <https://github.com/ploomber/projects/tree/master/cookbook/python-load>`_.