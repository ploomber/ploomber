Patterns
========


DAG factory
***********

To avoid redundancy, there should be a single DAG definition, such definition
should be taken as a single source of truth.

e.g. change location to isolate runs


Parametrized DAGs
*****************


Dynamic DAGs
************

Get daily dumps


Maximize code reusability
*************************

.. code-block:: python

    from my_pipeline import make_dag

    dag = make_dag()

    # reading this way ensures consistency accross your code
    df = pd.read_parquet(str(dag['make_data']))




Templating, clients, on_finish hook, DAG report, tasks library

how do we test task A works as expected?

code quality: catching bugs, static analysis, 

Multiple developers
*******************

Use Env, each developer has their own env.yaml so products do not overlap with
each other

Multiple systems
****************

Use Env, each machine has their own env.yaml so products do not overlap with
each other

