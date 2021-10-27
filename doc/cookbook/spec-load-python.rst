Load ``pipeline.yaml`` into Python
==================================

.. note::

    For a complete code example `click here <https://github.com/ploomber/projects/tree/master/cookbook/python-load>`_

When writing a pipeline via a ``pipeline.yaml`` file, the most common way to
interact is via the command-line interface (e.g., calling ``ploomber build``).
However, in some cases, we may want to add more logic to it, or embed it as part
of a larger script.

Ploomber represents pipelines using ``DAG`` objects. You can convert your
``pipeline.yaml`` to a ``DAG`` object in Python with the following code:

.. code-block:: python
    :class: text-editor

    from ploomber.spec import DAGSpec

    spec = DAGSpec('pipeline.yaml')
    dag = spec.to_dag()


The ``spec`` variable is an object of type :class:`ploomber.spec.DAGSpec`, while
``dag`` is of type :class:`ploomber.DAG`, to learn more about their interfaces,
click on any of the links.
