Introduction
============

There are three core concepts in ``ploomber``: :class:`Tasks <ploomber.tasks>`,
:class:`Products <ploomber.products>` and :class:`DAGs <ploomber.DAG>`. Tasks
are units of work that generate Products, Tasks dependencies are organized in
a DAG.

Most workflow management tools (such as Airflow) consist only of DAGs (or
similar) and Tasks, ploomber makes Products explicit since a workflow is not
only about what code to execute but what side-effects the workflow execution
will have once its done. Having this full specification make any workflow
transparent, which greatly helps debugging during the development phase.

Let's do a simple example:

.. ipython:: python

    from ploomber import DAG
    from ploomber.products import File
    from ploomber.tasks import PythonCallable

    def _make_data(product):
        return [1, 2, 3]

    dag = DAG()

    file = File('/tmp/dataset.parquet')
    make_data = PythonCallable(_make_data, file, dag, 'make_data')

    dag.status()



Goals
-----

Accelerate development
**********************

Incremental builds - on source code changes


Enable interactive execution
****************************

.. ipython:: python

    dag['make_data']#.build()


Ease debugging
**************

.. ipython:: python

    dag['make_data'].product
    # dag['make_data'].source
    dag.status()
    # execution summary


Maximize code reusability
*************************

.. code-block:: python

    from my_pipeline import make_dag

    dag = make_dag()

    # reading this way ensures consistency accross your code
    df = pd.read_parquet(str(dag['make_data']))


Transparency
************

plot and dag.status


Advanced features
-----------------

Templating, dynamic dags, clients, on_finish hook, DAG report, tasks library

