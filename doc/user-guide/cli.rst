Command line interface
======================

Once you assemble a pipeline and start developing, you'd want to see how
changes to the source code affect the pipeline's output. Ploomber provides
a command line interface to do this and to perform other common tasks. Here
are the most common commands.

Build pipeline (skips up-to-date tasks):

.. code-block:: console

    ploomber build


Forced build (runs all tasks):

.. code-block:: console

    ploomber build --force


Generate pipeline plot:

.. code-block:: console

    ploomber plot


By default, all commands assume your pipeline is defined in a ``pipeline.yaml``
file in the current directory. The ``pipeline.yaml`` file is known as "entry
point", there are other types of entry points, which we explain in the next
section.

Entry points
------------

There are three types of entry points (ordered by flexibility):

1. Directory (a directory with scripts)
2. Spec (aka ``pipeline.yaml``)
3. Factory function (a function that returns a ``DAG`` object)

For more information on entry points, see :doc:`/user-guide/spec-vs-python`.

Directory
*********

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


Spec
****

This is the default entry point because it allows a greater level of
flexibility without requiring you to write Python code to specify your
pipeline. It is a good choice for simple to moderately sophisticated pipelines.

It is a ``YAML`` file with certain structure, for schema details see:
:doc:`../api/spec`.


Factory function
****************

This is the most flexible type of entry point. A factory function is a Python
function that returns a DAG object. Factory functions are a good choice for
pipelines that require a high level of flexibility than a ``YAML`` file
cannot easily express. However it requires you to write your pipeline using
the Python API.

.. code-block:: python
    :class: text-editor
    :name: factory-py

    from ploomber import DAG

    def make():
        dag = DAG()
        # add tasks to your pipeline...
        return dag


To use it in the command line interface, use the ``--entry-point/-e`` and pass
a dotted path to the function. Assuming ``factory.py`` is importable in the
current directory, you can build the pipeline with the following command:

.. code-block:: console

    ploomber build --entry-point factory.make


Where to go next
****************

The command line interface is a convenient way to quickly iterate pipeline
development, just modify your source code and use the CLI to see results.

In some cases, we don't want our pipeline to be static but to have input
parameters determine its behavior. Go to the next guide to see how you can
add parameters to your pipeline and use the CLI to pass different parameter
values.
