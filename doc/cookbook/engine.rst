Engines
=======

Ploomber currently supports 2 engines for running notebooks:

- Ploomber Engine
- Papermill Engine


`Ploomber Engine <https://engine.ploomber.io/en/latest/quick-start.html>`_
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ploomber Engine is an in-house engine developed on top of Papermill, 
with better support for error debugging and pipeline deployment. 
Developers can use Ploomber Engine with Ploomber by setting the ``ploomber_engine`` argument to ``True`` 
in ``NotebookRunner``. Additionally, developers can pass the following `arguments <https://engine.ploomber.io/en/latest/api/api.html#execute-notebook>`_ in ``nb_executor_params`` while using ``NotebookRunner`` with ``ploomber-engine``.


`Papermill <https://papermill.readthedocs.io/en/latest/>`_
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Papermill is a Python library that provides functionality for 
parameterizing, executing, and analyzing Jupyter notebooks. 
With Papermill, developers can programmatically interact with notebooks, allowing 
for automation and customization of notebook workflows. Additionally, 
Papermill supports custom engines, which can be built on top of its APIs, 
and has been used for ``debugging`` in Ploomber. Please refer to the short `guide <https://docs.ploomber.io/en/latest/cookbook/debugging.html>`_ for more details on debugging.

By default, if the ``engine_name`` argument is not specified in 
``NotebookRunner``, and ``ploomber_engine`` is set to ``False``, 
Ploomber will use Papermill to execute notebooks.

Additionally, developers can pass the following `Papermill arguments <https://papermill.readthedocs.io/en/latest/reference/papermill-workflow.html?highlight=execute_notebook#module-papermill.execute>`_ in ``nb_executor_params`` while using ``NotebookRunner``.

**Note**: When migrating from one engine to another, make sure to check the parameters 
passed in ``nb_executor_params``, as different engines might have different arguments and functionalities.

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