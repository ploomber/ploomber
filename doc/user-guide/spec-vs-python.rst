Spec API vs. Python API
=======================

There are two ways of writing pipelines with Ploomber. This document discusses
the differences and how to decide which API to use.

Data projects span a wide range of applications, from small projects requiring
a few scripts to large ones requiring greater flexibility.

If you're getting started, the :ref:`Spec API <Spec entry point>` is
recommended. The Spec API is flexible enough to handle many common use cases
without requiring you to learn a Python API, you can get started quickly and get
pretty far using some of the advanced feature, check out the full
:doc:`documentation <../api/spec>` for details.

However, the Spec API is static, meaning that your ``pipeline.yaml``
completely describes your pipeline's structure. Under some circumstances, you
may want your pipeline to be more "dynamic." For example, you may use some
input parameters and create a "pipeline factory," which is a function that
takes those input parameters and creates a pipeline, allowing you to morph
the specifics of your pipeline (number of tasks, dependencies among them, etc.)
dynamically, you can only achieve so via
the :ref:`Python API is a <Factory entry point>`. The downside is that it has
a steeper learning curve.

There is a third way of assembling a pipeline by pointing to a :ref:`directory <Directory entry point>` with
scripts. This API allows you quickly test simple pipelines that may only have
a couple of tasks.

For examples using different APIs, `click here <https://github.com/ploomber/projects>`_

Depending on the API you use, the pipeline will be exposed to Ploomber
differently. For example, if using the Spec API, you tell the pipeline to
Ploomber by pointing to the path to a ``pipeline.yaml`` file, known
as an entry point, discussed below.

Entry points
------------

To execute your pipeline, Ploomber needs to know where it is. This location is
known as "entry point". There are three types of entry points (ordered by
flexibility):

1. A directory
2. [Spec API] Spec (aka ``pipeline.yaml``)
3. [Python API] Factory function (a function that returns a ``DAG`` object)

The following sections describe each entry point in detail.

Directory entry point
---------------------

Ploomber can figure out your pipeline without even having a ``pipeline.yaml``,
by just passing a directory. This kind of entry point is the simplest one but
also, the less flexible, since there isn't a pipeline definition, **products
must be declared in the source code itself**. Note that only Python and R
scripts support this, for example:

.. code-block:: python
    :class: text-editor
    :name: task-py

    # %% tags=["parameters"]
    product = {'nb': 'output.ipynb', 'data': 'output.csv'}
    upstream = ['a_task']
    # 

    # continues...


Internally, Ploomber uses the :py:mod:`ploomber.spec.DAGSpec.from_directory`
method. See the documentation for details.

All commands that accept the ``--entry-point/-e`` parameter can take a
directory as a value. For example, to build a pipeline using the current
directory:

.. code-block:: console

    ploomber build --entry-point .

It's also possible to select a subset of the files in a directory using a
glob-like pattern:


.. code-block:: console

    ploomber build --entry-point "*.py" # note the quotes

**Note:** Pipelines built without a ``pipeline.yaml`` file cannot be parametrized.

[Spec API] Spec entry point
----------------------------

If you want to customize how Ploomber executes your pipeline,
you have to create a ``pipeline.yaml`` file; this is known as a
**spec entry point**. A ``pipeline.yaml`` file is the recommended approach for
most projects: it has a good level of flexibility and doesn't require you to
learn Ploomber's internal Python API.

To call a DAG defined in a ``path/to/pipeline.yaml`` file pass the path:

.. code-block:: console

    ploomber build --entry-point pah/to/pipeline.yaml

If your pipeline exists inside a package:

.. code-block:: console

    ploomber build --entry-point my_package::pah/to/pipeline.yaml

The command above searches for package ``my_package`` (by doing ``import my_package``), then uses the relative path.

You can omit the ``--entry-point`` argument if the ``pipeline.yaml`` is in a standard location (:ref:`api-cli-default-locations`).

An added feature is pipeline parametrization, to learn more :doc:`/user-guide/parametrized`.

For schema details see: :doc:`../api/spec`.

[Python API] Factory entry point
--------------------------------

The last approach requires you to write Python code to specify your pipeline.
It has a steeper learning curve because you have to become familiar with the
API specifics, but it provides the most significant level of flexibility.

The primary advantage is dynamic pipelines, whose exact number of tasks
and dependency relations are determined when executing your Python code.
For example, you might use a for loop to dynamically generate a few tasks
based on some input parameters.

For Ploomber to know how to build your pipeline written as Python code, you have
to provide a **factory entry point**, which is a function that returns a
``DAG`` object. For example, if your factory is a function called `make` in
a file called ``pipeline.py``, then your entry point is the dotted path
``pipeline.make``, which may look like this:

.. code-block:: python
    :class: text-editor
    :name: factory-py

    from ploomber import DAG

    def make():
        dag = DAG()
        # add tasks to your pipeline...
        return dag


You can execute commands against your pipeline like this:


.. code-block:: console

    ploomber {command} --entry-point pipeline.make


Internally, Ploomber will do something like this:

.. code-block:: python
    :class: text-editor

    from pipeline import make

    dag = make()

    # (if using ploomber build)
    dag.build()


If your factory function has arguments, they will show up in the CLI. This
guide shows how to parametrize a factory
function: :doc:`../user-guide/parametrized`

If your factory function has a docstring, the first line displays
in the CLI help menu (e.g. ``ploomber build --entry-point factory.make --help``). If
the docstring is in
the `numpydoc format <https://numpydoc.readthedocs.io/en/latest/format.html#docstring-standard>`_
(and numpydoc is installed, ``pip install numpydoc``), descriptions for
documented parameters will be displayed as well.
