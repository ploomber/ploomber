"""
Ploomber spec API provides a quick and simple way to use Ploomber using YAML,
it is not indented to replace the Python API which offers more flexibility and
advanced features.

To understand the spec API you have to know a few basic concepts: "tasks" are
scripts that generate "products" (which can be local files or tables/views in
a database). If a task B has task A as an "upstream" dependency, it means B
uses product(s) from A as inputs.

`Click here for a live demo. <https://mybinder.org/v2/gh/ploomber/projects/master?filepath=spec%2FREADME.md>`_

To create a new project with basic structure:

.. code-block:: bash

    ploomber new

To add tasks to an existing project, update your pipeline.yaml file and execute:

.. code-block:: bash

    ploomber add


Build pipeline:

.. code-block:: bash

    python entry pipeline.yaml

To start an interactive session:

.. code-block:: bash

    ipython -i -m ploomber.entry pipeline.yaml -- --action status

Once the interactive session opens, use the ``dag`` object.


Visualize dependencies:

.. code-block:: python

    dag.plot()

Develop scripts interactively:

.. code-block:: python

    dag['some_task'].develop()


Line by line debugging:

.. code-block:: python

    dag['some_task'].debug()


Print the rendered source code from SQL scripts:


.. code-block:: python

    print(dag['some_sql_task'].source)


``pipeline.yaml`` schema
------------------------

Values within {curly brackets} contain explanations, otherwise, they represent
default values.

.. code-block:: yaml

    # pipeline.yaml
    meta:
        # inspect source code to extract product
        extract_product: False

        # inspect source code to extract upstream dependencies
        extract_upstream: True

        # Make paths in File products relative to their sources, otherwise
        # they are relative to the current working directory, defaults to
        # False
        product_relative_to_source: False

        # if any task does not have a "product_class" key, it will look up this
        # dictionary using the task class
        product_default_class:
            SQLDump: File
            NotebookRunner: File
            SQLScript: SQLRelation

        # Reload your pipeline every time you open a Jupyter notebook. Only
        # applies if the server extension is enabled. Not recommended for
        # large pipelines
        jupyter_hot_reload: False

    # For allowed keys and values see ploomber.DAGConfigurator
    config:
        {config-key}: {config-value}

    # clients are objects that connect to databases
    clients:
        {task or product class name}: {dotted.path.to.function}

    tasks:
        - {task dictionary, see below}


Notes
-----
* The meta section and clients is optional.
* The spec can also just be a list of tasks for DAGs that don't use clients and do not need to modify meta default values.
* If using a factory, the spec can just be

.. code-block:: yaml

    # pipeline.yaml
    location: {dotted.path.to.factory}

``task`` schema
---------------

.. code-block:: yaml

    # Any of the classes available in the tasks module
    # If missing, it will be inferred from "source".
    # NotebookRunner for .py and .ipynb files, SQLScript for .sql
    # and ShellScript for .sh
    class: {task class, optional}

    source: {path to source file}

    # Products that will be generated upon task execution. Should not exist
    # if meta.extract_product is set to True. This can be a dictionary if
    # the task generates more than one product. Required if extract_product
    # is False
    product: {str or dict}

    # Any of the classes available in the products module, if missing, the
    # class is looked up in meta.product_default_class using the task class
    product_class: {str, optional}

    # Optional task name, if missing, the value passed in "source" is used
    # as name
    name: {task name, optional}

    # Dotted path to a function that has no parameters and returns the
    # client to use. By default the class-level client at config.clients is
    # used, this value overrides it. Only required for tasks that require
    # clients
    client: {dotted.path.to.function, optional}

    # Same as "client" but applies to the product, most of the time, this will
    # be the same as "client". See the FAQ for more information (link at the
    # bottom)
    product_client: {dotted.path.to.function, optional}

    # Dependencies for this task. Only required if meta.extract_upstream is
    # set to True
    upstream: {str or list, optional}

    # Function to execute when the task renders successfully
    on_render: {dotted.path.to.function, optional}

    # Function to execute when the task finishes successfully
    on_finish: {dotted.path.to.function, optional}

    # Function to execute when the task fails
    on_failure: {dotted.path.to.function, optional}

    # NOTE: All remaining values are passed to the task constructor as keyword
    arguments


Click here to go to :doc:`faq_index/`.

Upstream dependencies
---------------------

The "upstream" key defined on each task plays an important role for pipeline
execution. Making task A and upstream dependency of task B (A --> B) means that
you want to use output(s) from A as inputs for B. This tells Ploomber to
execute A before B, but it also makes output from A available to B.


Python scripts details
======================

If your task is a script, this leads to a cell injection. Say you have a script
that looks like this:

.. code-block:: python

    # annotated python file (converted to a notebook during execution)

    # + tags=["parameters"]
    # this script depends on the output generated by a task named "clean"
    upstream = {'clean'}
    product = 'path/to/output/b.csv'

    # +
    # rest of your code...


This is not the code that Ploomber executes, but rather one with an injected
cell:

.. code-block:: python

    # annotated python file (converted to a notebook during execution)

    # + tags=["parameters"]
    # this script depends on the output generated by a task named "clean"
    upstream = {'clean'}
    product = 'path/to/output/b.csv'

    # + tags=["injected-parameters"]
    # ploomber makes the output from A available in B)
    upstream = {'clean': 'path/to/output/a.csv'}

    # +
    # rest of your code...

This makes your code consistent, you only have to declare outputs once, and
they're automatically propagated to their downstream consumers.

To eliminate the gap between the code you write and the one that gets executed,
a Jupyter notebook extension is provided. Which automatically injects
parameters and deletes them before saving changes. To enable it:

.. code-block:: bash

    jupyter serverextension enable ploomber

To disable it:

.. code-block:: bash

    jupyter serverextension disable ploomber


`Click here for documentation on Jupyter extensions <https://jupyter-notebook.readthedocs.io/en/stable/examples/Notebook/Distributing%20Jupyter%20Extensions%20as%20Python%20Packages.html#Enable-a-Server-Extension`_


SQL scripts details
===================

When tasks execute SQL, upstream dependencies are propagated in the upstream
dictionary:

.. code-block:: sql

    DROP TABLE IF EXISTS {{product}};

    CREATE TABLE {{product}} AS
    -- this task depends on the output generated by a task named "clean"
    SELECT * FROM {{upstream['clean']}}
    WHERE x > 10

"""
