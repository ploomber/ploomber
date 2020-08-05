Entry points
------------

An entry point allows Ploomber to construct an internal representation of your
pipeline for execution. The most common kind of entry point is a
``pipeline.yaml`` file. Which describes your tasks and pipeline settings, but
this is not the only way to let Ploomber know about your pipeline, this guide
explains alternative ways.


Directory
=========

Ploomber can figure out your pipeline without even having a ``pipeline.yaml``,
by just passing a directory. This kind of entry point is the simplest one but
also the less flexible, it is a good option for small pipelines where you
don't need to customize settings.

Internally, Ploomber uses the :class:`ploomber.spec.DAGSpec.from_directory`
method. See the documentation for details on how a directory is converted into a
pipeline.

All commands that accept the ``--entry-point/-e`` parameter, can take a
directory as a value. For example, to build a pipeline using the current
directory:

.. code-block:: console

    ploomber build --entry-point .



Factory function
================

This is the most flexible type of entry point. A factory function is a Python
function that returns a DAG object.


.. code-block:: python
    :class: text-editor
    :name: factory-py

    from ploomber import DAG

    def make():
        dag = DAG()
        # add tasks to your pipeline...
        return dag

The example above uses the the ``DAG`` class to explicitly assemble the dag,
but you can use any method you want. As long as your function returns a ``DAG``
instance, it is considered a valid factory function. For example, you could
build the ``DAG`` indirectly by building a ``DAGSpec`` first and then call
the ``.to_dag()`` method to get a ``DAG`` instance.

To use it in the command line interface, use the ``--entry-point/-e`` and pass
a dotted path to the function. Assuming ``factory.py`` is importable in the
current directory, you can build the pipeline with the following command:

.. code-block:: console

    ploomber build --entry-point factory.make


Parametrized factories
**********************

If your factory takes parameters, they'll also be available in the command
line interface. Function parameters with no default values are converted to
positional arguments and function parameters with default values are converted
to optional parameters. To see the exact auto-generated API, you can use the
``--help`` command:


.. code-block:: console

    ploomber build --entry-point factory.make --help


Furthermore, you can use
`type hints <https://docs.python.org/3/library/typing.html>`_ to automatically
cast your command line parameters to the appropriate type:

.. code-block:: python
    :class: text-editor
    :name: factory-w-params-py

    from ploomber import DAG

    def make(param: str, another: int = 10):
        dag = DAG()
        # add tasks to your pipeline...
        return dag
