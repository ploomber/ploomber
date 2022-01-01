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


Feature engineering with SQL
****************************

With modern data-warehouses such as Snowflake, using SQL for feature engineering
can significantly simplify the development process since the warehouse takes care of
scaling your code.

You can use Ploomber and SQL to iterate on your feature engineering code
quickly, then download the features to train a model using Python.

`Here's an example <https://github.com/ploomber/projects/tree/master/templates/spec-api-sql>`_ that applies
transformations in a database, dumps a table to a local file, and then processes it using Python.

Uploading batch predictions to a database
*****************************************

If you're working on a Machine Learning whose predictions must be uploaded to a
database table, you can implement this with Ploomber.

ETL
***

Ploomber allows you to easily write SQL pipelines. `Here's an ETL example <https://github.com/ploomber/projects/tree/master/templates/etl>`_.