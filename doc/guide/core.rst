Core concepts
-------------

Ploomber's core: DAGs, Tasks and Products
=========================================

To get started with ploomber you only have to learn three concepts:

1. Task. A unit of work that takes some input and produces a persistent change
2. Product. A persistent change *produced* by a Task (e.g. a file in the local filesystem, a table in a remote database)
3. DAG. A collection of Tasks used to specify dependencies among them (use output from Task A as input for Task B)

There is an universal Task API defined in a :class:`Task abstract class <ploomber.tasks.Task>`, concrete classes
must adhere to this API but can have extra parameters in their constructor,
the same applies for Product, there is an :class:`Product abstract class <ploomber.products.Product>` that defines their API.

The DAG lifecycle: Declare, render, execute
===========================================

A DAG goes through three steps before being executed:

1. Declaration. A DAG is created and Tasks are added to it
2. Rendering. Placeholders are resolved and validation is performed on Task inputs
3. Execution. All *outdated* Tasks are executed in the appropriate order (run upstream task dependencies first)

Declaration
***********


.. ipython:: python

    from pathlib import Path
    import pandas as pd
    from ploomber import DAG
    from ploomber.tasks import PythonCallable, SQLUpload
    from ploomber.clients import SQLAlchemyClient
    from ploomber.products import File, SQLiteRelation


The simplest Task is PythonCallable, which takes a callable (e.g. a function) as its first argument. The only requirement for the functions is to have a product
argument, if the task has dependencies, it must have an upstream argument as well.

.. ipython:: python

    def _one_task(product):
        pd.DataFrame({'one_column': [1, 2, 3]}).to_csv(str(product))

.. ipython:: python

    def _another_task(upstream, product):
        df = pd.read_csv(str(upstream['one_task']))
        df['another_column'] = df['one_column'] + 1

.. ipython:: python

    # instantiate dag
    dag = DAG()


.. ipython:: python

    # instantiate two tasks and add them to the DAG
    one_task = PythonCallable(_one_task, File('one_file'), dag, 'one')
    another_task = PythonCallable(_another_task, File('another_file'), dag, 'another')
    # declare dependencies: another_task depends on one_task
    one_task >> another_task

Rendering
*********

Rendering is a pre-processing step where placeholders are resolved and
a few validation checks are run. Placeholders exist to succinctly declare
DAGs, once a parameter is declared, it can be used in several contexts. Placeholders are extremely useful for propagating values in a DAG.

Once you declare a dependency, you make the product(s) of the upstream Task
available to the downstream Task at render time, this allows you to only
define a product once and propagate it to downstream tasks.

Within the downstream task, you can access upstream dependencies using jinja syntax via the `upstream` key. Any parameter passed to the Task via the
`param` parameter, will also be available at rendering time.


.. ipython:: python

    client = SQLAlchemyClient('sqlite:///my_db.db')
    dag.clients[SQLUpload] = client
    dag.clients[SQLiteRelation] = client


.. ipython:: python
    
    sql_task = SQLUpload('{{upstream["another"]}}',
                         SQLiteRelation((None, '{{upstream["another"].name}}', 'table')),
                         dag, 'sql')
    another_task >> sql_task



.. ipython:: python

    dag.render()
    sql_task

.. talk about debugging rendering

.. talk about source code tracking, parameter passing