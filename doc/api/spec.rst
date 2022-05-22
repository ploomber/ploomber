Spec API (``pipeline.yaml``)
============================

.. note::
    
    This document assumes you are already familiar with Ploomber's core concepts (DAG, product, task, and upstream). If you're not, check out this guide: :doc:`../get-started/basic-concepts`.


This section describes the ``pipeline.yaml`` schema.

``meta``
--------

``meta`` is an optional section for meta-configuration, it controls how the
DAG is constructed.

``meta.source_loader``
**********************


Load task sources (``tasks[*].source``) from a Python module. For example,
say you have a module ``my_module`` and want to load sources from a
``path/to/sources`` directory inside that module:

.. code-block:: yaml
    :class: text-editor

    meta:
        source_loader:
            module: my_module
            path: path/to/sources


``meta.import_tasks_from``
**************************

Add tasks defined in a different file to the current one. This direcive is
useful for composing pipelines. For example, if you have a training and a
serving pipeline, you can define the pre-processing logic in a
``pipeline.preprocessing.yaml`` and then import the file into
``pipeline.training.yaml`` and ``pipeline.serving.yaml``:

.. code-block:: yaml
    :class: text-editor

    meta:
        import_tasks_from: /path/to/tasks.yaml


The file must be a list where each element is a valid Task.

`Click here <https://github.com/ploomber/projects/tree/master/templates/ml-intermediate>`_ to see a batch serving example.

`Click here <https://github.com/ploomber/projects/tree/master/templates/ml-online>`_ to see an online serving example.


``meta.extract_upstream``
*************************

Extract upstream dependencies from the source code (``True`` by default).

.. code-block:: yaml
    :class: text-editor

    meta:
        extract_upstream: True


If False, tasks must declare dependencies using the ``upstream`` key:

.. code-block:: yaml
    :class: text-editor

    meta:
        extract_upstream: false

    tasks:
        - source: tasks/clean.py
          product: outupt/report.html
          upstream: [some_task, another_task]


``meta.extract_product``
************************

Default:

.. code-block:: yaml
    :class: text-editor

    meta:
        extract_product: False

``meta.product_default_class``
******************************

Product class key for a given task class. Names should match (case-sensitive)
the names in the Python API. These are rarely changed, except
for ``SQLScript``. Defaults:

.. code-block:: yaml
    :class: text-editor

    meta:
        product_default_class:
            SQLScript: SQLRelation
            SQLDump: File
            NotebookRunner: File
            ShellScript: File
            PythonCallable: File


``executor``
************

Determines which executor to use:

1. ``serial``: Runs one task at a time (Note: By default, function tasks run in a subprocess)
2. ``parallel``: Run independent tasks in parallel (Note: this runs all tasks in a subprocess)
3. Dotted path: This allows you to customize the initialization parameters

For example, say you want to use the :class:`ploomber.executors.Serial` executor
but do not want to run functions in a subprocess, you can pass a dotted path
and custom parameters like this:

.. code-block:: yaml
    :class: text-editor

    executor:
      dotted_path: ploomber.executors.Serial
      build_in_subprocess: false # do not run function tasks in a subprocess


Another common use case is to limit the number of subprocesses when using the
:class:`ploomber.executors.Parallel` executor:


.. code-block:: yaml
    :class: text-editor

    executor:
      dotted_path: ploomber.executors.Parallel
      processes: 2 # limit to a max of 2 processes


To learn more about the executors:

* :class:`ploomber.executors.Serial`
* :class:`ploomber.executors.Parallel`

``clients``
***********

These are the default clients. It allows you to specify
a single client for all Tasks/Products for a given class. The most common
use case is SQL database configuration.

Other scenarios are :py:mod:`ploomber.products.File` clients, which Ploomber can use
to backup pipeline results (say, for example, you run a job that trains
several models and want to save output results. You can use
:py:mod:`ploomber.clients.GCloudStorageClient` or :py:mod:`ploomber.clients.S3Client` for that.


Keys must be valid :py:mod:`ploomber.tasks` or :py:mod:`ploomber.products`
names, values must be dotted paths to functions that return a
:py:mod:`ploomber.clients` instance.

Can be a string (call without arguments):

.. code-block:: yaml
    :class: text-editor
    :name: task-client-string-yaml

    clients:
        # this assumes there is a clients.py with a get_client function
        {some-class}: clients.get_client

Or a dictionary (to call with arguments):

.. code-block:: yaml
    :class: text-editor
    :name: task-client-dict-yaml

    clients:
        {some-class}:
            # this assumes there is a clients.py with a get_client function
            dotted_path: clients.get_client
            kwarg_1: value_1
            ...
            kwarg_k: value_k


.. collapse:: Example: Database dump

    .. literalinclude:: ../../../projects-ploomber/cookbook/sql-dump/pipeline.yaml
        :class: text-editor
        :language: yaml


    .. literalinclude:: ../../../projects-ploomber/cookbook/sql-dump/clients.py
        :class: text-editor
        :language: python

    Download:

    .. code-block:: console

        ploomber examples -n cookbook/sql-dump -o sql-dump


.. collapse:: Example: Upload files to the cloud

    .. literalinclude:: ../../../projects-ploomber/cookbook/file-client/pipeline.yaml
        :class: text-editor
        :language: yaml


    .. literalinclude:: ../../../projects-ploomber/cookbook/file-client/clients.py
        :class: text-editor
        :language: python


    Download:

    .. code-block:: console

        ploomber examples -n cookbook/file-client -o file-client


.. collapse:: Full projects

    * `SQL pipeline <https://github.com/ploomber/projects/tree/master/templates/spec-api-sql>`_
    * `Example using BigQuery and Cloud Storage. <https://github.com/ploomber/projects/tree/master/templates/google-cloud>`_


.. _on-render-finish-failure:

``on_{render, finish, failure}``
********************************

.. important::

    Hooks are **not** executed when opening scripts/notebooks
    in :doc:`Jupyter. <../user-guide/jupyter>`


These are hooks that execute when specific events happen:

1. ``on_render``: executes after verifying there are no errors in your pipeline declaration (e.g., a task that doesn't exist declared as an upstream dependency)
2. ``on_finish``: executes upon successful pipeline run
3. ``on_failure``: executes upon failed pipeline run

They all are optional and take a dotted path as an argument. For example,
assume you have a ``hooks.py`` with function ``on_render``, ``on_finish``,
and ``on_failure``. You can add them to your ``pipeline.yaml`` like this:

.. code-block:: yaml
    :class: text-editor

    on_render: hooks.on_render
    on_finish: hooks.on_finish
    on_failure: hooks.on_failure


If your hook takes arguments, you may call it like this:

.. code-block:: yaml
    :class: text-editor

    # to call any hook with arguments
    # {hook-name} must be one of: on_render, on_finish, on_failure
    {hook-name}:
        dotted_path: {dotted.path.to.hook}
        argument: value

Calling with arguments is useful when you have :doc:`a parametrized pipeline <../user-guide/parametrized>`.

If you need information from your DAG in your hook, you may
request the ``dag`` (:class:`ploomber.DAG`) argument in any of the
hooks. ``on_finish`` can also request a ``report`` argument, which constains a
summary report of the pipeline's execution.

``on_failure`` can request a ``traceback``
argument which will have a dictionary, possible keys are ``build`` which
has the build error traceback, and ``on_finish`` which includes the
``on_finish`` hook traceback, if any. For more information, see the DAG
documentation :class:`ploomber.DAG`.


.. collapse:: Example: Hooks

    .. literalinclude:: ../../../projects-ploomber/cookbook/hooks/pipeline.yaml
        :class: text-editor
        :language: yaml
        :lines: 1-6


    .. literalinclude:: ../../../projects-ploomber/cookbook/hooks/hooks.py
        :class: text-editor
        :language: python
        :lines: 6-25

    Download:

    .. code-block:: console

        ploomber examples -n cookbook/hooks -o hooks


.. _serializer-and-unserializer:

``serializer`` and ``unserializer``
***********************************

By default,  tasks whose source is a function
(i.e., :py:mod:`ploomber.tasks.PythonCallable`). Receive input paths
(in ``upstream``) and output paths (in ``product``) when the function executes. Saving interim results allows Ploomber to provide incremental
builds (:ref:`incremental-builds`).

However, in some cases, we might want to provide a pipeline that performs
all operations in memory (e.g., to do online serving).
:py:mod:`ploomber.OnlineDAG` can convert a file-based pipeline
into an in-memory one without code changes, allowing you to re-use your
feature engineering code for training and serving. The only requisite is for
tasks to configure a ``serializer`` and ``unserializer``.
`Click here <https://github.com/ploomber/projects/tree/master/templates/ml-online>`_ to
see an example.

Normally, a task whose source is a function looks like this:

.. code-block:: py
    :class: text-editor

    import pandas as pd

    def my_task(product, upstream):
        df_upstream = pd.read_csv(upstream['name'])
        # process data...
        # save product
        df_product.to_csv(product)

And you use the ``product`` parameter to save any task output.

However, if you add a ``serializer``, ``product`` isn't passed, and you must
return the product object:

.. code-block:: py
    :class: text-editor

    import pandas as pd

    def my_task(upstream):
        df_upstream = pd.read_csv(upstream['name'])
        # process data...
        return df_product

The ``serializer`` function is called with the returned object as its
first argument and ``product`` (output path) as the second argument:

.. code-block:: py
    :class: text-editor

    serializer(df_product, product)


A similar logic applies to ``unserializer``; when present, the function is
called for each upstream dependency with the product as the argument:

.. code-block:: py
    :class: text-editor

    unserializer(product)

In your task function, you receive objects (instead of paths):

.. code-block:: py
    :class: text-editor

    import pandas as pd

    def my_task(upstream):
        # no need to call pd.read_csv here
        df_upstream = upstream['name']
        return df_product

If you want to provide a Task-level serializer/unserializer pass it directly to
the task, if you set a DAG-level serializer/unserializer and wish to exclude
specific task pass ``serializer: null`` or ``unserializer: null`` in the
selected task.

.. collapse:: Example: Serialization

    .. literalinclude:: ../../../projects-ploomber/cookbook/serialization/pipeline.yaml
        :class: text-editor
        :language: yaml


    .. literalinclude:: ../../../projects-ploomber/cookbook/serialization/util.py
        :class: text-editor
        :language: python

    Download:

    .. code-block:: console

        ploomber examples -n cookbook/serialization -o serialization

``source_loader``
*****************

If you package your project (i.e., add a ``setup.py``), ``source_loader`` offers
a convenient way to load sources inside such package.

For example, if your package is named ``my_package`` and you want to load from
the folder ``my_sources/`` within the package:

.. code-block:: yaml
    :class: text-editor

    meta:
        source_loader:
            module: my_package
            path: my_sources

    tasks:
        # this is loaded from my_package (my_sources directory)
        - source: script.sql
          # task definition continues...

To find out the location used, you can execute the following in a Python
session:

.. code-block:: python
    :class: ipython

    import my_package; print(my_package) # print package location


The above should print something like ``path/to/my_package/__init__.py``.
Using the configuration above, it implies that source loader will load the file
from ``path/to/my_package/my_sources/script.sql``.

**Note:** this only applies to tasks whose ``source`` is a relative path. Dotted
paths and absolute paths are not affected.

For details, see :py:mod:`ploomber.SourceLoader`, which is the underlying Python
implementation. `Here's an example that uses source_loader <https://github.com/ploomber/projects/blob/master/templates/ml-online/src/ml_online/pipeline.yaml>`_.

SQLScript product class
***********************

By default, SQL scripts use :py:mod:`ploomber.products.SQLRelation` as
product class. Such product doesn't save product's metadata; required for
incremental builds (:ref:`incremental-builds`). If you want to use them, you
need to change the default value and configure the product's client.

`Here's an example <https://github.com/ploomber/projects/tree/master/templates/spec-api-sql>`_
that uses ``product_default_class`` to configure a SQLite pipeline with
incremental builds.

For more information on product clients, see: :doc:`../user-guide/faq_index`.


Loading from a factory
**********************

The CLI looks for a ``pipeline.yaml`` by default, if you're using the Python API,
and want to save some typing, you can specify a ``pipeline.yaml`` like this:

.. code-block:: yaml
    :class: text-editor

    # pipeline.yaml
    location: {dotted.path.to.factory}

With such configuration, commands such as ``ploomber build`` will work.


``task``
--------

``task`` schema.


.. tip::

    All other keys passed here are forwarded to the class constructor, so the
    allowed values will depend on the task class. For example, if running a
    notebook the task class is :class:`ploomber.tasks.NotebookRunner`, if it's
    a function it'll be a :class:`ploomber.tasks.PythonCallable`, see the
    documentation to learn what extra arguments they take.

``tasks[*].name``
*****************

The name of the task. The filename (without the extension) is used if not
defined.

``tasks[*].source``
*******************

Indicates where the source code for a task is. This can be a path to a files if
using scripts/notebooks or dotted paths if using a function.

By default, paths are relative to the ``pipeline.yaml`` parent folder (absolute
paths are not affected), unless ``source_loader`` is configured; in such
situation, paths are relative to the location configured in the
``SourceLoader`` object. See the ``source_loader`` section for more details.

For example, if your pipeline is located at ``project/pipeline.yaml``, and
you have:

.. code-block:: yaml
    :class: text-editor

    tasks:
        - source: scripts/my_script.py
          # task definition continues...

Ploomber will expect your script to be located at
``project/scripts/my_script.py``


If using a function, the dotted path should be importable. for example, if
you have:

.. code-block:: yaml
    :class: text-editor

    tasks:
        - source: my_package.my_module.my_function
          # task definition continues...

Ploomber runs a code equivalent to:


.. code-block:: py
    :class: text-editor

    from my_package.my_module import my_function


``tasks[*].product``
********************

Indicates output(s) generated by the task. This can be either a File(s) or
SQL relation(s) (table or view). The exact type depends on the ``source`` value
for the given task: SQL scripts generate SQL relations, everything else
generates files.

When generating files, paths are relative to the ``pipeline.yaml`` parent
directory. For example, if your pipeline is located at
``project/pipeline.yaml``, and you have:

.. code-block:: yaml
    :class: text-editor

    tasks:
        - source: scripts/my_script.py
          product: output/my_output.csv

Ploomber will save your output to ``project/output/my_output.csv``


When generating SQL relations, the format is different:

.. code-block:: yaml
    :class: text-editor

    tasks:
        - source: scripts/my_script.sql
          # list with three elements (last one can be table or view)
          product: [schema, name, table]
          # schema is optional, it can also be: [name, table]


If the task generates multiple products, pass a dictionary:


.. code-block:: yaml
    :class: text-editor

    tasks:
        - source: scripts/my_script.py
          product:
            nb: output/report.html
            data: output/data.csv


The mechanism to make ``product`` available when exeuting your task
depends on the type of task.

SQL tasks receive a ``{{product}}`` placeholder:

.. code-block:: postgresql
    :class: text-editor

    -- {{product}} is replaced by "schema.name" or "name" if schema is empty
    CREATE TABLE {{product}} AS
    SELECT * FROM my_table WHERE my_column > 10

If ``product`` is a dictionary, use ``{{product['key']}}``

Python/R scripts/notebooks receive a ``product`` variable in the
"injected-parameters" cell:

.. code-block:: py
    :class: text-editor

    # %% tags=["parameters"]
    product = None

    # %% tags=["injected-parameters"]
    product = '/path/to/output/data.csv'

    # your code...


If ``product`` is a dictionary, this becomes
``product = {'key': '/path/to/output/data.csv', ...}``

Python functions receive the ``product`` argument:

.. code-block:: py
    :class: text-editor

    import pandas as pd

    def my_task(product):
        # process data...
        df.to_csv(product)

If ``product`` is a dictionary, use ``product['key']``.


The same logic applies when making ``upstream`` dependencies available to
tasks, but in this case. ``upstream`` is always a dictionary: SQL scripts can
refer to their upstream dependencies using ``{{upstream['key']}}``. While
Python scripts and notebooks receive upstream in the "injected-parameters"
cell, and Python functions are called with an ``upstream`` argument.


``tasks[*].params``
*******************

Use this section to pass arbitrary parameters to a task. The exact mechanism
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

Python/R scripts/notebooks receive them in the "injected-parameters" cell:

.. code-block:: py
    :class: text-editor

    # %% tags=["parameters"]
    my_param = None

    # %% tags=["injected-parameters"]
    my_param = 42

    # your code...


Python functions receive them as arguments:

.. code-block:: py
    :class: text-editor


    # function is called with my_param=42
    def my_task(product, my_param):
        pass

.. _tasks-on-render-finish-failure:

``tasks[*].on_{render, finish, failure}``
*****************************************

.. important::

    Hooks are **not** executed when opening scripts/notebooks
    in :doc:`Jupyter. <../user-guide/jupyter>`


These are hooks that execute under certain events. They are equivalent to
:ref:`dag-level hooks <on-render-finish-failure>`, except they apply to a
specific task. There are three types of hooks:

1. ``on_render`` executes right before executing the task.
2. ``on_finish`` executes when a task finishes successfully.
3. ``on_failure`` executes when a task errors during execution.

They all are optional and take a dotted path as an argument. For example,
assume your ``hooks.py`` with functions ``on_render``, ``on_finish``, and
``on_failure``. You can add those hooks to a task in your ``pipeline.yaml``
like this:

.. code-block:: yaml
    :class: text-editor

    tasks:
        - source: tasks.my_task
          product: products/output.csv
          on_render: hooks.on_render
          on_finish: hooks.on_finish
          on_failure: hooks.on_failure

If your hook takes arguments, you may call it like this:

.. code-block:: yaml
    :class: text-editor

    # to call any hook with arguments
    # {hook-name} must be one of: on_render, on_finish, on_failure
    {hook-name}:
        dotted_path: {dotted.path.to.hook}
        argument: value

Calling with arguments is useful when you have :doc:`a parametrized pipeline <../user-guide/parametrized>`.

If you need information from the task, you may add any of the following
arguments to the hook:

1. ``task``: Task object (a subclass of  :class:`ploomber.tasks.Task`)
2. ``client``: Tasks's client (a subclass of  :class:`ploomber.clients.Client`)
3. ``product``: Tasks's product (a subclass of  :class:`ploomber.products.Product`)
4. ``params``: Tasks's params (a dictionary)

For example, if you want to check the data quality of a function that cleans some data, you may want to add an ``on_finish`` hook that loads the output and tests the data:

.. code-block:: python
    :class: text-editor

    import pandas as pd

    def on_finish(product):
        df = pd.read_csv(product)

        # check that column "age" has no NAs
        assert not df.age.isna().sum()

.. collapse:: Example: Hooks

    .. literalinclude:: ../../../projects-ploomber/cookbook/hooks/pipeline.yaml
        :class: text-editor
        :language: yaml
        :lines: 8-16

    .. literalinclude:: ../../../projects-ploomber/cookbook/hooks/hooks.py
        :class: text-editor
        :language: python
        :lines: 28-46

    Download:

    .. code-block:: console

        ploomber examples -n cookbook/hooks -o hooks

.. _tasks-params-resources:

``tasks[*].params.resources_``
******************************

The ``params`` section contains an optional section called ``resources_`` (Note
the trailing underscore). By default, Ploomber marks tasks as outdated when
their parameters change; however, parameters in the ``resources_``
section work differently: they're marked as outdated when the contents of the file
change. For example, suppose you're using a JSON file as a configuration
source for a given task, and want to make Ploomber re-run a task if such file
changes, you can do something like this:

.. code-block:: yaml
    :class: text-editor

    tasks:
        - source: scripts/my-script.py
          product: report.html
          params:
            resources_:
                # whenever the JSON file changes, my-script.py runs again
                file: my-config-file.json


.. _tasks-grid:

``tasks[*].grid``
*****************

Sometimes, you may want to run the same task over a set of parameters, ``grid``
allows you to do so. For example, say you want to train multiple models, each
one with a different set of parameters:

.. code-block:: yaml
    :class: text-editor
    :name: grid-example-1-yaml

    tasks:
      - source: random-forest.py
        # name is required when using grid
        name: random-forest-
        product: random-forest.html
        grid:
            n_estimators: [5, 10, 20]
            criterion: [gini, entropy]

The spec above generates nine tasks for each combination of parameters with
products ``random-forest-X.html`` where ``X`` goes from ``0`` to ``8``. Task
names also include a suffix.

You can also customize the product outputs to organize them in different
folders and names (**Added in version 0.17.2**):

.. code-block:: yaml
    :class: text-editor
    :name: grid-example-2-yaml

    tasks:
      - source: random-forest.py
        name: random-forest-
        product: 'n_estimators=[[n_estimators]]/criterion=[[criterion]].html'
        grid:
            n_estimators: [5, 10, 20]
            criterion: [gini, entropy]


The example above will generate outputs by replacing the parameter values;
for example, it will store the random forest with ``n_estimators=5``, and
``criterion=gini`` at, ``n_estimators=5/criterion=gini.html``. Note that this
uses square brackets to differentiate them from regular placeholders when
using an ``env.yaml`` file.

You may pass a list instead of a dictionary to use multiple sets of parameters:

.. code-block:: yaml
    :class: text-editor
    :name: grid-example-3-yaml

    tasks:
      - source: train-model.py
        name: train-model-
        product: train-model.html
        grid:
          - model_type: [random-forest]
            n_estimators: [5, 10, 20]
            criterion: [gini, entropy]

          - model_type: [ada-boost]
            n_estimators: [1, 3, 5]
            learning_rate: [1, 2]

To create a task downstream to all tasks generated by ``grid``, you can use a
wildcard (e.g., ``train-model-*``).

.. collapse:: Example: Grid

    .. literalinclude:: ../../../projects-ploomber/cookbook/grid/pipeline.yaml
        :class: text-editor
        :language: yaml
        :lines: 17-35

    Download:

    .. code-block:: console

        ploomber examples -n cookbook/grid -o grid


.. collapse:: Example: Model selection with nested cross-validation


    .. literalinclude:: ../../../projects-ploomber/cookbook/nested-cv/pipeline.yaml
        :class: text-editor
        :language: yaml

    Download:

    .. code-block:: console

        pip install ploomber
        ploomber examples -n cookbook/nested-cv -o nested-cv

.. collapse:: Changelog

    .. versionadded:: 0.17.2
        Use ``params`` and ``grid`` in the same task. Values in ``params`` are constant across the grid.

    .. versionadded:: 0.17.2
        Cstomize the product paths with placeholders ``[[placeholder]]``


``tasks[*].client``
*******************

Task client to use. By default, the class-level client in the ``clients``
section is used. This task-level value overrides it. Required for some
tasks (e.g., ``SQLScript``), optional for others (e.g., ``File``).

Can be a string (call without arguments):

.. code-block:: yaml
    :class: text-editor
    :name: task-client-string-yaml

    client: clients.get_db_client

Or a dictionary (to call with arguments):

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

Product client to use (to save product's metadata). Only required if you want
to enable incremental builds (:ref:`incremental-builds`) if using SQL products.
It can be a string or a dictionary (API is the same as ``tasks[*].client``).

More information on product clients: :doc:`../user-guide/faq_index`.


``tasks[*].upstream``
*********************

Dependencies for this task. Only required if ``meta.extract_upstream=True``

.. code-block:: yaml
    :class: text-editor

    tasks:
        ...
        upstream: {str or list}


**Example:**

.. code-block:: yaml
    :class: text-editor

    tasks:
        source: scripts/my-script.py
        product: output/report.html
        upstream: [clean_data_a, clean_data_b]


``tasks[*].class``
*****************

Task class to use (any class from ploomber.tasks). You rarely have to set
this, since it is inferred from ``source``. For example,
:class:`ploomber.tasks.NotebookRunner` for ``.py`` and ``.ipynb``
files, :class:`ploomber.tasks.SQLScript` for ``.sql``, and
:class:`ploomber.tasks.PythonCallable` for dotted paths.

``tasks[*].product_class``
**************************

This takes any class name from :class:`ploomber.products`. You rarely have
to set this, since values from ``meta.product_default_class`` contain the
typical values.



Parametrizing with ``env.yaml``
-------------------------------

In some situations, it's helpful to parametrize a pipeline. For example, you
could run your pipeline with a sample of the data as a smoke test; to make
sure it runs before triggering a run with the entire dataset, which could take
several hours to finish.


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

In the previous example, ``module.function`` is called with
``my_param='my_value'`` and ``my_second_param='another_value'``.

A common pattern is to use a pipeline parameter to change the location of
``tasks[*].product``. For example:

.. code-block:: yaml
    :class: text-editor

    tasks:
        - source: module.function
          # path determined by a parameter
          product: '{{some_directory}}/output.csv'              

        - source: my_script.sql
          # schema and prefix determined by a parameter
          product: ['{{some_schema}}', '{{some_prefix}}_name', table] 

This can help you keep products generated by runs with different parameters in
different locations.

These are the most common use cases, but you can use placeholders anywhere in
your ``pipeline.yaml`` values (not keys):

.. code-block:: yaml
    :class: text-editor

    tasks:
        - source: module.function
          # doesn't work
          '{{placeholder}}': value


You can update your ``env.yaml`` file or switch them from the command-line to
change the parameter values, run ``ploomber build --help`` to get a list of
arguments you can pass to override the parameters defined in ``env.yaml``.

Note that these parameters are constant (they must be changed explicitly by you
either by updating the ``env.yaml`` file or via the command line), if you want
to define dynamic parameters, you can do so with the Python API,
`check out this example <https://github.com/ploomber/projects/tree/master/cookbook/dynamic-params>`_ for an
example.


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

There are a few default placeholders you can use in your ``pipeline.yaml``,
even if not defined in the ``env.yaml`` (or if you don't have a ``env.yaml`` altogether)

* ``{{here}}``: Absolute path to the parent folder of ``pipeline.yaml``
* ``{{cwd}}``: Absolute path to the current working directory
* ``{{root}}``: Absolute path to project's root folder. It is usually the same as ``{{here}}``, except when the project is a package (i.e., it has ``setup.py`` file), in such a case, it points to the parent directory of the ``setup.py`` file.
* ``{{user}}``: Current username
* ``{{now}}``: Current timestamp in ISO 8601 format (*Added in Ploomber 0.13.4*)
* ``{{git_hash}}``: git tag (if any) or git hash (*Added in Ploomber 0.17.1*)
* ``{{git}}``: returns the branch name (if at the tip of it), git tag (if any), or git hash (*Added in Ploomber 0.17.1*)


A common use case for this is when passing paths to files to scripts/notebooks. For example, let's say your script has to read a file from a specific location. Using ``{{here}}`` turns path into absolute so you can ready it when using Jupyter, even if the script is in a different location than your ``pipeline.yaml``.


By default, paths in ``tasks[*].product`` are interpreted relative to the
parent folder of ``pipeline.yaml``. You can use  ``{{cwd}}`` or ``{{root}}``
to override this behavior:

.. code-block:: yaml
    :class: text-editor

    tasks:
        - source: scripts/my-script.py
          product:
            nb: products/report.html
            data: product/data.csv
          params:
            # make this an absolute file so you can read it when opening
            # scripts/my-script.py in Jupyter
            input_path: '{{here}}/some/path/file.json'

For more on parametrized pipelines, check out the guide: :doc:`../user-guide/parametrized`.
