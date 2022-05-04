Configuration (``dev``/``prod``)
============================================

In the previous guide (:doc:`../user-guide/parametrized`), we saw how to use an
``env.yaml`` file to parametrize our pipeline and switch parameters from the
command line.

Sometimes we want to change all the parameters at once. The most common
scenario is to change configuration during development and production.

For example, say you're working on a Machine Learning pipeline whose
``pipeline.yaml`` looks like this:

.. code-block:: yaml
    :class: text-editor
    :name: pipeline-yaml

    tasks:

      - source: get.py
        product:
          nb: get.ipynb
          data: raw.csv
        params:
          sample_pct: '{{sample_pct}}'

      - source: get.py
        product:
          nb: get.ipynb
          data: raw.csv

      - source: get.py
        product:
          nb: get.ipynb
          data: raw.csv


The pipeline above has one placeholder ``'{{sample_pct}}'``, which controls
which percentage of raw data to download. You may want to develop locally with a
fraction of the data, say 20%, to iterate quickly. To
`smoke test <https://en.wikipedia.org/wiki/Smoke_testing_(software)>`_ quickly,
you may run it with a smaller sample, say 1%. Finally, to train a model, you'll
use 100% of the data.

.. tip::

    You can use placeholders (e.g., ``{{sample_pct}}``) anywhere in the
    ``pipeline.yaml`` file. Another typical use case is to switch the product
    location (e.g., ``product: '{{product_directory}}/some-data.csv'``.


By default, Ploomber looks for an ``env.yaml``. To enable rapid local
development with 20% of the data, you may create an ``env.yaml`` file like this:

.. code-block:: yaml
    :class: text-editor

    sample_pct: 20

For smoke testing, ``env.test.yaml``:

.. code-block:: yaml
    :class: text-editor

    sample_pct: 1

And for training, ``env.train.yaml``:

.. code-block:: yaml
    :class: text-editor

    sample_pct: 100

To switch configurations, you can set the ``PLOOMBER_ENV_FILENAME`` environment variable
to ``env.test.yaml`` in the testing environment and to ``env.train.yaml`` in
the training environment.

Whenever ``PLOOMBER_ENV_FILENAME`` has a value, Ploomber uses it and looks for a file
with such a name. Note that this must be a filename, not a path since Ploomber
expects ``env.yaml`` files to exist in the same folder as the ``pipeline.yaml``
file. For example, if you're on Linux or macOS:

.. code-block:: console

    export PLOOMBER_ENV_FILENAME=env.train.yaml && ploomber build


.. important::

    If you're using the Jupyter integration and want to see the changes
    reflected in the injected cell, you need to shut down Jupyter
    set ``PLOOMBER_ENV_FILENAME``, and start Jupyter again.


Managing multiple pipelines
---------------------------

If your project has more than one pipeline, they'll likely need
different ``env.yaml`` files.

Say you have two pipelines, one for training a model (``pipeline.yaml``) and
one for serving it (``pipeline.serve.yaml``). You can create an ``env.yaml``
file to parametrize ``pipeline.yaml`` and an ``env.serve.yaml`` to parametrize
``pipeline.serve.yaml``:

.. code-block:: sh

    project/
        pipeline.yaml
        pipeline.serve.yaml
        env.yaml
        env.serve.yaml

The general rule is as follows: When loading a ``pipeline.{name}.yaml``,
extract the ``{name}`` portion. Then look for a ``env.{name}.yaml`` file, if
such file doesn't exist, look for an ``env.yaml`` file. Note that the
``PLOOMBER_ENV_FILENAME`` environment variable overrides this process.

Alternatively, you may separate the pipelines into different directories, and
put an ``env.yaml`` on each one:

.. code-block:: sh

    project-a/
        pipeline.yaml
        env.yaml
    project-b/
        pipeline.yaml
        env.yaml


``env.yaml`` composition (DRY)
------------------------------

.. note:: New in version 0.18

In many cases, your development and production environment configuration share
many values in common. To keep them simple, you may create an ``env.yaml``
(development configuration) and have your ``env.prod.yaml`` (production
configuration) inherit from it:

.. code-block:: yaml
    :class: text-editor
    :name: env-yaml

    key: value
    key_another: dev-value


Then in your ``env.prod.yaml``:

.. code-block:: yaml
    :class: text-editor

    meta:
      # import development config
      import_from: env.yaml

    # no need to declare key: value here, it'll be imported from env.yaml

    # overwrite value
    key_another: production-value

Note that if the value in ``import_from`` is a relative path, it is considered
so relative to the location of the env file (in our case ``env.prod.yaml``).

You can switch values in ``env.yaml`` from the command line, to see how:

.. code-block:: console

    ploomber build --help


Example, if you have a ``key`` in your ``env.yaml``:

.. code-block:: console

    ploomber build --env--key new-value
