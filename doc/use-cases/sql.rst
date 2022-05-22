SQL Pipelines
=============

Ploomber comes with built-in support for SQL. You provide SQL scripts and
Ploomber manages connections to the database and orchestrates execution for you.

.. raw:: html

    <div class="mermaid">
    graph LR
        ca[Clean table A] --> ta[Transform] --> m[Merge] 
        cb[Clean table B] --> tb[Transform] --> m
        m --> Dump --> Plot
    </div>


Process with SQL and Python
****************************

With data warehouses such as Snowflake, using SQL for transforming data
can significantly simplify the development process since the warehouse takes care of
scaling your code.

You can use Ploomber and SQL to process large datasets
quickly, then download the data to continue your analysis with Python for
plotting or training a Machine Learning model

.. collapse:: Example: BigQuery and Cloud Storage pipeline

    .. code-block:: console

        pip install ploomber
        ploomber examples -n templates/google-cloud -o google-cloud

.. collapse:: Example: SQL pipeline (transform with SQL, and plot with Python)

    .. code-block:: console

        pip install ploomber
        ploomber examples -n templates/spec-api-sql -o spec-api-sql


Uploading batch predictions to a database
*****************************************

If you're working on a Machine Learning whose predictions must be uploaded to a
database table, you can implement this with Ploomber.

ETL
***

Ploomber allows you to write ETL SQL pipelines.

.. collapse:: Example: ETL pipeline

    .. code-block:: console

        pip install ploomber
        ploomber examples -n templates/etl -o etl

