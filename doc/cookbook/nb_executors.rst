Notebook Executors
==================

Ploomber currently supports two notebook executors:

- `Papermill <https://papermill.readthedocs.io/en/latest/>`_
- `Ploomber Engine <https://engine.ploomber.io/en/latest/quick-start.html>`_


Papermill(default)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Papermill allows you to parameterizing, executing, and analyzing Jupyter notebooks. 
By default, if the ``executor`` argument is not specified in 
``NotebookRunner``, Ploomber will use Papermill to execute notebooks.
Additionally, you can pass the following `Papermill arguments <https://papermill.readthedocs.io/en/latest/reference/papermill-workflow.html?highlight=execute_notebook#module-papermill.execute>`_ in ``executor_params`` while using ``NotebookRunner``.

**Note**: When migrating from one executor to another, make sure to check the parameters 
passed in ``executor_params``, as different executors might have different arguments and functionalities.

Sample pipeline


.. code-block:: yaml
    :class: text-editor

    tasks:
        # By default, papermill engine is used for executing the scripts
        # source is the code you want to execute (.ipynb also supported)
      - source: get.py
        # products are task's outputs
        product:
          # scripts generate executed notebooks as outputs
          nb: output/1-get.html
          # you can define as many outputs as you want
          data: output/raw_data.csv
        # Selecting the executor for notebook
        executor: papermill
        # Executor params: Here passed to papermill
        executor_params:
            log_output: True

Ploomber-Engine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ploomber-Engine is a notebook executor developer by the Ploomber team with better support for debugging and deployment. 
You can use Ploomber-Engine with Ploomber by setting the ``executor`` argument to ``ploomber-engine`` 
in ``NotebookRunner``. Additionally, you can pass the following `arguments <https://engine.ploomber.io/en/latest/api/api.html#execute-notebook>`_ in ``executor_params`` while using ``NotebookRunner`` with ``ploomber-engine``.


Sample pipeline

.. code-block:: yaml
    :class: text-editor

    tasks:
        # By default, papermill engine is used for executing the scripts
        # source is the code you want to execute (.ipynb also supported)
      - source: get.py
        # products are task's outputs
        product:
          # scripts generate executed notebooks as outputs
          nb: output/1-get.html
          # you can define as many outputs as you want
          data: output/raw_data.csv
        # Selecting the executor for notebook
        executor: ploomber-engine
        # Executor params: Here passed to Ploomber-Engine
        executor_params:
            log_output: True