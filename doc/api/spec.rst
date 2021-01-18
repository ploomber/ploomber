Spec API
========

**Note:** The `projects repository <https://github.com/ploomber/projects>`_
contains several ``pipeline.yaml`` examples.


``pipeline.yaml`` schema
------------------------

(Values displayed are defaults)

.. code-block:: yaml
    :class: text-editor
    :name: pipeline-yaml

    meta:
        # inspect source code to extract product. If False, tasks must have a
        # "product" key
        extract_product: True

        # inspect source code to extract upstream dependencies. If False, tasks
        # must contain an "upstream" key (if the task has dependencies)
        extract_upstream: True

        # Make paths in File products relative to their sources, otherwise
        # they are relative to the folder where pipeline.yaml is located, defaults
        # to False
        product_relative_to_source: False

        # if any task does not have a "product_class" key, it will look up this
        # dictionary using the task class
        product_default_class:
            SQLDump: File
            NotebookRunner: File
            ShellScript: File
            PythonCallable: File
            SQLScript: SQLRelation

        # Reload your pipeline every time you open a Jupyter notebook. Only
        # applies if the server extension is enabled. Not recommended for
        # large pipelines
        jupyter_hot_reload: False

        # Show tasks, whose source is a function, as notebooks in jupyter
        jupyter_functions_as_notebooks: False


    # DAG configuration
    config:
        # For allowed keys and values see ploomber.DAGConfigurator
        {config-key}: {config-value}

    # DAG clients
    clients:
        # Clients for connecting to databases
        {task or product class name}: {dotted.path.to.function}
        # Example:
        SQLScript: db.get_client
        PostgresRelation: db.get_client

    tasks:
        - {task dictionary, see below}
        # Example (notebook task)
        - source: clean_data.py
          # assuming meta.extract_product: False
          product:
            nb: output/clean_data.ipynb
            data: output/clean.csv
          # assuming meta.extract_upstream: False
          upstream: [get_raw_data]


Notes
-----
* The meta section and clients is optional.
* The spec can also just be a list of tasks for DAGs that don't use clients and do not need to modify meta default values.
* If using a factory, the spec can just be

.. code-block:: yaml
    :class: text-editor

    # pipeline.yaml
    location: {dotted.path.to.factory}

``task`` schema
---------------

.. code-block:: yaml
    :class: text-editor
    :name: task-schema-yaml

    # Any of the classes available in the tasks module
    # If missing, it will be inferred from "source".
    # NotebookRunner for .py and .ipynb files, SQLScript for .sql
    # and ShellScript for .sh
    class: {task class, optional}

    # Path to script (for Python/R notebooks, bash or SQL), dotted path to
    # execute Python functions
    source: {path/to/source/file or dotted.path.to.function}

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

    # Similar to "client" but applies to the product, most of the time, this will
    # be the same as "client". See the FAQ for more information (link at the
    # bottom)
    product_client: {dotted.path.to.function, optional}

    # Dependencies for this task. Only required if meta.extract_upstream is
    # set to True. If False, only required if the task has dependencies
    upstream: {str or list, optional}

    # Function to execute when the task renders successfully
    on_render: {dotted.path.to.function, optional}

    # Function to execute when the task finishes successfully
    on_finish: {dotted.path.to.function, optional}

    # Function to execute when the task fails
    on_failure: {dotted.path.to.function, optional}

    # NOTE: All remaining values are passed to the task constructor as keyword arguments


Click here to go to :doc:`../user-guide/faq_index`.
