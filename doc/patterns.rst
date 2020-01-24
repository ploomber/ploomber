Patterns
========

Maximize code reusability
*************************

.. code-block:: python

    from my_pipeline import make_dag

    dag = make_dag()

    # reading this way ensures consistency accross your code
    df = pd.read_parquet(str(dag['make_data']))




Templating, dynamic dags, clients, on_finish hook, DAG report, tasks library

how do we test task A works as expected?

code quality: catching bugs, static analysis, 