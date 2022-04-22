Downloading templates
=====================

Use our pre-configured templates as a starting point for your projects.

Selected templates
------------------

.. tip::

    Click on the template link to see the source code on GitHub. Once there, you'll see an option to launch a free, hosted JupyterLab.

* Exploratory Data Analysis
    1. *Basic EDA example*: Load and clean data. Then create HTML reports with visualizations. (`templates/exploratory-analysis <https://github.com/ploomber/projects/tree/master/templates/exploratory-analysis>`_)
* Machine Learning
    1. *Basic ML example*: Get data, clean it, and train a model. (`templates/ml-basic <https://github.com/ploomber/projects/tree/master/templates/ml-basic>`_)
    2. *Intermediate ML example*: Create training and batch serving pipelines. (`templates/ml-intermediate <https://github.com/ploomber/projects/tree/master/templates/ml-intermediate>`_)
    3. *Online API*: Deploy pipeline as an API using Flask. (`templates/ml-online <https://github.com/ploomber/projects/tree/master/templates/ml-online>`_)
    4. *Experiment grid + Mlflow*: Create a grid of experiments and track them with MLflow. (`templates/mlflow <https://github.com/ploomber/projects/tree/master/templates/mlflow>`_)
* SQL databases
    1. *Basic SQL example*: Process data, dump it, and visualize with Python. (`templates/spec-api-sql <https://github.com/ploomber/projects/tree/master/templates/spec-api-sql>`_)
    2. *ETL*: dump data from remote storage, upload it to a database, process it, and visualize it with Python. (`templates/etl <https://github.com/ploomber/projects/tree/master/templates/etl>`_)


Downloading a template
----------------------

To download any of the examples:

.. code-block:: console

    ploomber examples -n {template} -o {output-directory}


For example, if you want to copy the *Basic EDA example* to the ``eda`` directory in your computer:


.. code-block:: console

    ploomber examples -n templates/exploratory-analysis -o eda


.. tip::
    
    Once you download an example, you can explore it with Jupyter. Check out
    the :doc:`/user-guide/jupyter` guide to learn more.

Once the download finishes, you'll need to install dependencies; you can use
the ``ploomber install`` command. You may call ``conda`` or ``pip`` directly.

Listing all templates
---------------------

To list all the available examples:


.. code-block:: console

    ploomber examples


Note that the command above will display three sections:

1. *Templates.* Pre-configured projects that you can use as a starting point.
2. *Cookbook.* Short examples to get something done quickly.
3. *Guides.* In-depth tutorials covering features in detail.

Note that both ``Cookbook`` and ``Guides`` are part of the documentation itself, and
you can navigate to any of them using the left sidebar or download them to run them locally.

Templates structure
-------------------

All templates follow the same structure:

1. ``README.md``: Instructions to run the template.
2. ``README.ipynb``: Same as ``README.md`` but in notebook format and with command outputs.
3. ``environment.yml``: ``conda`` dependencies file.
4. ``requirements.txt``: ``pip`` dependencies file.


Most templates contain a ``pipeline.yaml`` file, so you can
run ``ploomber build`` to execute the pipeline, but there are a few exceptions.
Check out the template's ``README.md`` for specifics.

Starting projects from scratch
------------------------------

If no template suits your needs, use the ``ploomber scaffold`` command
to create a clean slate project. :doc:`Click here to learn how to scaffold projects </user-guide/scaffold>`.

``ploomber scaffold`` also comes with utilities to modify existing pipelines,
so can use it to change any of the templates.