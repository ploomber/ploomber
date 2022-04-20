Batch processing
================

You can export Ploomber pipelines to production schedulers for batch
processing. Check out our package
`Soopervisor <https://soopervisor.readthedocs.io>`_, which
allows you to export to
`Kubernetes <https://soopervisor.readthedocs.io/en/latest/tutorials/kubernetes.html>`_
(via `Argo workflows <https://argoproj.github.io/>`_),
`AWS Batch <https://soopervisor.readthedocs.io/en/latest/tutorials/aws-batch.html>`_,
`Airflow <https://soopervisor.readthedocs.io/en/latest/tutorials/airflow.html>`_,
and `SLURM <https://soopervisor.readthedocs.io/en/latest/tutorials/slurm.html>`_.

Composing batch pipelines
*************************

To compose a batch pipeline, use the ``import_tasks_from`` directive in
your ``pipeline.yaml`` file.

For example, define your feature generation tasks in a ``features.yaml`` file:


.. code-block:: yaml
    :class: text-editor
    :name: features-yaml

    # generate one feature...
    - source: features.a_feature
      product: features/a_feature.csv

    # another feature...
    - source: features.anoter_feature
      product: features/another_feature.csv

    # join the two previous features...
    - source: features.join
      product: features/all.csv
        

Then import those tasks in your training pipeline, ``pipeline.yaml``:


.. code-block:: yaml
    :class: text-editor
    :name: pipeline-yaml

    meta:
        # import feature generation tasks
        import_tasks_from: features.yaml

    tasks:
        # Get raw data for training
        - source: train.get_historical_data
          product: raw/get.csv
        
        # The import_tasks_from injects your features generation tasks here

        # Train a model
        - source: train.train_model
          product: model/model.pickle

Your serving pipeline ``pipepline-serve.yaml`` would look like this:

.. code-block:: yaml
    :class: text-editor
    :name: pipeline-serve-yaml

    meta:
        # import feature generation tasks
        import_tasks_from: features.yaml

    tasks:
        # Get new data for predictions
        - source: serve.get_new_data
          product: serve/get.parquet
        
        # The import_tasks_from injects your features generation tasks here

        # Make predictions using a trained model
        - source: serve.predict
          product: serve/predictions.csv
          params:
            path_to_model: model.pickle

`Here's an example
<https://github.com/ploomber/projects/tree/master/templates/ml-intermediate>`_ project
showing how to use ``import_tasks_from`` to create a training
(``pipeline.yaml``) and serving (``pipeline-serve.yaml``) pipeline.


Scheduling
**********

For an example showing how to schedule runs with cron and Ploomber, `click here. <https://github.com/ploomber/projects/tree/master/guides/cron>`_