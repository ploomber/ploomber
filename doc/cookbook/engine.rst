Engines
=======

Ploomber currently supports 2 engines for running notebooks:

- Papermill Engine
- Ploomber Engine

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

`Ploomber Engine <https://engine.ploomber.io/en/latest/quick-start.html>`_
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ploomber Engine is an in-house engine developed on top of Papermill, 
with better support for error debugging and pipeline deployment. 
Developers can use Ploomber Engine with Ploomber by setting the ``ploomber_engine`` argument to ``True`` 
in ``NotebookRunner``. Additionally, developers can pass the following `arguments <https://engine.ploomber.io/en/latest/api/api.html#execute-notebook>`_ in ``nb_executor_params`` while using ``NotebookRunner`` with ``ploomber-engine``.

**Note**: When migrating from one engine to another, make sure to check the parameters 
passed in ``nb_executor_params``, as different engines might have different arguments and functionalities.