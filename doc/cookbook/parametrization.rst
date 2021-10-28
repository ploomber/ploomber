Parametrization
===============

.. note:: This is a quick reference, for an in-depth tutorial, :doc:`click here <../user-guide/parametrized>`.

To parametrize your pipeline, create an ``env.yaml``:

.. code-block:: yaml
    :class: text-editor
    :name: env-yaml

    some_param: some_value
    another_param: 42


Then use ``{{placeholders}}`` in your ``pipeline.yaml`` file:


.. code-block:: yaml
    :class: text-editor
    :name: pipeline-yaml

    tasks:
      - source: scripts/plot.py
        product: products/plot.ipynb
        params:
          some_param: '{{some_param}}'

When executing your pipeline, ``scripts/plot.py`` receives
``some_param="some_value"``.

You can use ``{{placeholders}}``. The most common use case
are ``tasks[*].params`` (just like the example above),
and ``tasks[*].product``, to change the output location, or you can use both at the same time:


.. code-block:: yaml
    :class: text-editor
    :name: pipeline-yaml

    tasks:
      - source: scripts/plot.py
        product: 'products/{{some_param}}/plot.ipynb'
        params:
          some_param: '{{some_param}}'


Switching from the command line
-------------------------------

Ploomber recognizes ``{{placeholders}}`` and adds command-line arguments to
change their value, to see a list of available placeholders:


.. code-block:: console

    ploomber build --help


Dynamic parameters
------------------

Parameters declared in ``env.yaml`` are static (they can only change in value
by editing the ``env.yaml`` file or via the command-line); however, you can use
the Python API to create dynamic parameters whose values are determined at
runtime, `check out this example <https://github.com/ploomber/projects/tree/master/cookbook/dynamic-params>`_.