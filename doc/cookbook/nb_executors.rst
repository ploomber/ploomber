Notebook Executors
=======

Ploomber currently supports two notebook executors:

- `Papermill <https://papermill.readthedocs.io/en/latest/>`_
- `Ploomber Engine <https://engine.ploomber.io/en/latest/quick-start.html>`_


Papermill(default)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Papermill allows you to parameterizing, executing, and analyzing Jupyter notebooks. 
By default, if the ``executor`` argument is not specified in 
``NotebookRunner``, Ploomber will use Papermill to execute notebooks.
Additionally, you can pass the following `Papermill arguments <https://papermill.readthedocs.io/en/latest/reference/papermill-workflow.html?highlight=execute_notebook#module-papermill.execute>`_ in ``executor_params`` while using ``NotebookRunner``.
**Note**: When migrating from one engine to another, make sure to check the parameters 
passed in ``executor_params``, as different engines might have different arguments and functionalities.

Sample pipeline


Ploomber
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ploomber Engine is a notebook executor developer by the Ploomber team with better support for debugging and deployment. 
You can use Ploomber Engine with Ploomber by setting the ``engine`` argument to ``ploomber-engine`` 
in ``NotebookRunner``. Additionally, you can pass the following `arguments <https://engine.ploomber.io/en/latest/api/api.html#execute-notebook>`_ in ``nb_executor_params`` while using ``NotebookRunner`` with ``ploomber-engine``.


Sample pipeline
~~~~~~~~~~~~~~~
For this cookbook, we are going to edit the pipeline the ``first-pipeline``. To run this locally, install `Ploomber <https://docs.ploomber.io/en/latest/get-started/quick-start.html>`_ and execute: ``ploomber examples -n guides/first-pipeline``.

Replace the existing ``pipeline.yaml`` with following content:

.. code-block:: yaml
    :class: text-editor

    tasks:
        # By default, papermill engine is used for executing the scripts
        # source is the code you want to execute (.ipynb also supported)
      - source: 1-get.py
        # products are task's outputs
        product:
          # scripts generate executed notebooks as outputs
          nb: output/1-get.html
          # you can define as many outputs as you want
          data: output/raw_data.csv

        # Explicity specifying the engine parameter
      - source: 2-profile-raw.py
        product: output/2-profile-raw.html
        engine: papermill

        # Using Ploomber-Engine 
      - source: 3-clean.py
        product:
            nb: output/3-clean.html
            data: output/clean_data.parquet
        engine: ploomber_engine

        # Passing params to papermill engine
      - source: 4-profile-clean.py
        product: output/4-profile-clean.html
        engine: papermill
        # nb_executor_params can be used for both
        # papermill and ploomber-engine
        nb_executor_params:
            log_output: True

        # Passing params to ploomber-engine
      - source: 5-plot.py
        product: output/5-plot.html
        engine: ploomber_engine
        nb_executor_params:
            log_output: True