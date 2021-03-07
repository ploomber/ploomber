Spec API (``pipeline.yaml``)
============================

This section describes how to specify pipelines using a ``pipeline.yaml`` file.

**Note:** The `projects repository <https://github.com/ploomber/projects>`_
contains several ``pipeline.yaml`` examples.


Quick reference
---------------

A typical ``pipeline.yaml`` looks like this:

.. code-block:: yaml
    :class: text-editor

    meta:
        extract_product: False

    tasks:
        - source: functions.get_raw_data
          product: output/raw.csv

        - source: scripts/plot.py
          product:
            nb: output/plots.html
            data: output/clean.csv

For each task, ``source`` specifies the source code for the task and
``product`` where to save the output (relative to the location of the
``pipeline.yaml`` file).


Schema
------

Under some situations, you might want to change the default configuration, a
common scenario happens when using SQL scripts: you have to configure a client
to connect to the database.

The complete schema is shown below (with default values):

.. code-block:: yaml
    :class: text-editor
    :name: pipeline-yaml

    # (optional section)
    meta:
        # load task sources (.tasks[*].source) using a ploomber.SourceLoader
        # See section below for details
        source_loader:
            # Example:
            # Load sources from my_module...
            module: my_module
            # [optional] ...use this path inside my_module
            path: path/to/sources/

        # Include tasks defined in a different file (must be a list where each
        # element is a valid Task. Useful for composing pipelines.
        # See section below for details
        import_tasks_from: /path/to/tasks.yaml

        # Reload your pipeline every time you open a Jupyter notebook. Not
        # recommended for large pipelines
        jupyter_hot_reload: False

        # Show function tasks as notebooks in jupyter
        jupyter_functions_as_notebooks: False

        # Default product class key for a given task class. Names should
        # match (case-sensitive) the names in the Python API. These are rarely
        # changed, except for SQLScript, see section below for details
        product_default_class:
            SQLScript: SQLRelation
            SQLDump: File
            NotebookRunner: File
            ShellScript: File
            PythonCallable: File

        # Extract product from source code. If False, tasks must have a "product" key
        extract_product: True

        # Extract upstream dependencies from source code. If False, tasks
        # must declare dependencies using the "upstream" key
        extract_upstream: True

        # Make paths in File products relative to their sources, otherwise
        # they are relative to the pipeline.yaml parent folder
        product_relative_to_source: False


    # DAG configuration (optional section)
    config:
        # For allowed keys and values see ploomber.DAGConfigurator
        {config-key}: {config-value}

    # DAG clients (optional section)
    clients:
        # Clients for connecting to databases
        {task or product class name}: {dotted.path.to.function}
        # Example (calls db.get_client without arguments)
        SQLScript: db.get_client
        # Call with arguments:
        PostgresRelation:
            dotted_path: db.get_client
            some_keyword_arg: value

    # DAG-level serializer/unserializer for Python functions (both optional)
    serializer: {dotted.path.to.serializer}
    unserializer: {dotted.path.to.unserializer}

    # (this section is required)
    tasks:
        - {task dictionary, see next section}
        # Example (notebook task)
        - source: clean_data.py
          # assuming meta.extract_product: False
          # and meta.extract_upstream: True
          product:
            nb: output/clean_data.ipynb
            data: output/clean.csv


``source_loader``
*****************

If you package your project (i.e., add a ``setup.py``), ``source_loader`` offers
a convenient way to load sources inside such package.

For example, if your package is named ``my_package`` and you want to load from
the folder ``my_sources/`` within the package:

.. code-block:: yaml
    :class: text-editor

    source_loader:
        module: my_package
        path: my_sources


For details, see :py:mod:`ploomber.SourceLoader`, which is the underlying Python
implementation.

SQLScript product class
***********************

By default, SQL scripts will use :py:mod:`ploomber.products.SQLRelation` as
product class. Such product does not save product's metadata, required for
incremental builds. If you want to use incremental builds, you need to change
the default value and configure the product's client.

For more information on product clients, see: :doc:`../user-guide/faq_index`.

``import_tasks_from``
*********************

When training a Machine Learning pipeline, we obtain raw data, generate
features and train a model. When serving, we receive new observations, generate
features and make predictions. Only the first and last steps change, everything
that happens in the middle is the same. ``import_tasks_from`` allows you to
compose pipelines for training and serving.

For example, you may define all your feature engineering code in a
``pipeline-features.yaml`` file. Then import those tasks (using
``import_tasks_from``) in a training pipeline (``pipeline.yaml``)
and a serving pipeline (``pipeline-serving.yaml``).

`Click here <https://github.com/ploomber/projects/tree/master/ml-online/src/ml_online>`_ to see an example.


Loading from a factory
**********************

The CLI looks for a pipeline.yaml by default, if you're using the Python API,
if you want to save some typing, you can specify a ``pipeline.yaml`` like this:

.. code-block:: yaml
    :class: text-editor

    # pipeline.yaml
    location: {dotted.path.to.factory}

With such configuration, commands such as ``ploomber build`` will work.


``task`` schema
---------------

.. code-block:: yaml
    :class: text-editor
    :name: task-schema-yaml


    # If task is a script (Python/R notebooks, bash or SQL): path to script
    # (paths are relative to the pipeline.yaml parent folder)
    # If task is a function: dotted path to function
    source: {path/to/source/file or dotted.path.to.function}

    # Task product. Required if meta.extract_product=False
    # Can be a dictionary if the task generates more than one product.
    product: {str or dict}

    # Task name. If missing, it is inferred from the task's source
    name: {task name, optional}

    # Function to execute when the task renders successfully
    on_render: {dotted.path.to.function, optional}

    # Function to execute when the task finishes successfully
    on_finish: {dotted.path.to.function, optional}

    # Function to execute when the task fails
    on_failure: {dotted.path.to.function, optional}

    # Task parameters. See section below for details
    params:
        {key}: {value}

    # Dotted path to a function that returns the task client.
    # See section below for details.
    client: {dotted.path.to.function, optional}

    # Dotted path to a function that returns the product client.
    # See section below for details.
    product_client: {dotted.path.to.function, optional}

    # Task class to use (any class from ploomber.tasks)
    # You rarely have to set this, since it is inferred from "source".
    # (e.g., NotebookRunner for .py and .ipynb files, SQLScript for .sql,
    # PythonCallable for dotted paths)
    class: {task class, optional}

    # Product class (any class from ploomber.products)
    # You rarely have to set this, since values from meta.product_default_class
    # contain the typical cases
    product_class: {str, optional}

    # Dependencies for this task. Only required if meta.extract_upstream=True
    upstream: {str or list, optional}

    # All remaining values are passed to the task constructor as keyword
    # arguments. See ploomber.tasks documentation for details


``tasks[*].params``
*******************

Use this section to pass arbitrary parameters to a task, the exact mechanism
depends on the task type. Assume you have the following:

.. code-block:: yaml
    :class: text-editor

    tasks:
        - source: {some-source}
          product: {some-product}
          params:
            my_param: 42

SQL tasks receive them as placeholders.

.. code-block:: postgresql
    :class: text-editor

    -- {{my_param}} is replaced by 42
    SELECT * FROM my_table WHERE my_column > {{my_param}}

Python/R scripts/notebnooks receive them in the "parameters" cell:

.. code-block:: py
    :class: text-editor

    # + tags=["parameters"]
    my_param = None

    # + tags=["injected-parameters"]
    my_param = 42

    # your code...


Python functions receive them as arguments:

.. code-block:: py
    :class: text-editor


    # function is called with my_param=42
    def my_task(product, my_param):
        pass


``tasks[*].client``
*******************

Task client to use. By default the class-level client at config.clients is
used, this value overrides it. Required for some tasks (e.g., ``SQLScript``),
optional for others (e.g., ``File``).

Can be a string (call without arguments):

.. code-block:: yaml
    :class: text-editor
    :name: task-client-string-yaml

    client: clients.get_db_client

Or a dictionary:


.. code-block:: yaml
    :class: text-editor
    :name: task-client-dict-yaml

    client:
        dotted_path: clients.get_db_client
        kwarg_1: value_1
        ...
        kwarg_k: value_k


``tasks[*].product_client``
***************************

Product client to use (where to save product's metadata to support
incremental builds). Only required if you want to enable incremental builds with
SQL products.

Can be a string or a dictionary (same as ``tasks[*].client``).

More information on product clients: :doc:`../user-guide/faq_index`.

Parametrizing with ``env.yaml``
-------------------------------

In some situations, it's useful to parametrize a pipeline, for example, you
could run your pipeline with a sample of the data as a quick smoke test and make
sure it runs before running it with the whole dataset, which could take several
hours to finish.


To add parameters to your pipeline, create and ``env.yaml`` file next to your
``pipeline.yaml``:


.. code-block:: yaml
    :class: text-editor
    :name: env-yaml

    my_param: my_value
    nested:
        param: another_value

Then use placeholders in your ``pipeline.yaml`` file:

.. code-block:: yaml
    :class: text-editor

    tasks:
        - source: module.function
          params:
            my_param: '{{my_param}}'
            my_second_param: '{{nested.param}}'

In the previous example, the function is called with ``my_param='my_value'``

You can include placeholders anywhere in your ``pipeline.yaml`` file.

Setting parameters from the CLI
*******************************

Once you define pipeline parameters, you can switch them from the command line:


.. code-block:: console

    ploomber {command} --env--param value # note the double dash


For example:

.. code-block:: console

    ploomber build --env--param value


Default placeholders
********************

There are default placeholders you can use in your ``pipeline.yaml``,
even if you don't define them in a ``env.yaml`` (or you don't even have one).


* ``{{here}}``: Absolute path to the parent folder of ``pipeline.yaml``
* ``{{cwd}}``: Absolute path to current working directory
* ``{{root}}``: Absolute path to project's root folder. Only available if there is a ``setup.py`` file in your project, the parent of such file is considered the project's root
* ``{{user}}``: Current username


By default, paths in ``tasks[*].product`` are interpreted relative to the
parent folder of ``pipeline.yaml``. You can use  ``{{cwd}}`` or ``{{root}}``
to override this behavior:

.. code-block:: yaml
    :class: text-editor

    tasks:
        - source: module.function
          product: '{{cwd}}/products/output.csv'

        - source: module.another_function
          product: '{{cwd}}/products/another_output.csv'

This ensures that no matter where your ``pipeline.yaml`` is, products will
be stored relative to the current working directory.

Check out the guide: :doc:`../user-guide/parametrized`.