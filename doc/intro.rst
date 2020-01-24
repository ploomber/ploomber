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

    import tempfile
    from pathlib import Path

    import pandas as pd

    from ploomber import DAG
    from ploomber.products import File
    from ploomber.tasks import PythonCallable

    def _make_data(product):
        df = pd.DataFrame({'a': [1, 2, 3]})
        df.to_parquet(str(product))
        # ipython directive complains if this is not here...
        return df

    dag = DAG()

    file = File(Path(tempfile.mkdtemp(), 'dataset.parquet'))
    make_data = PythonCallable(_make_data, file, dag, 'make_data')

    dag.status()



Goals
-----

ploomber is designed to accelerate experimentation and facilitate a transition
to a production system.

Accelerate experimentation
**************************

Data processing pipelines consist on many small long-running tasks which
depend on each other. During early development phases things are expected to
change: new tasks are added, bugs are fixed. Triggering a full end-to-end
run on each change is wasteful. On a successful run, ploomber saves the task
source code, if the pipeline is run again, it will skip tasks that are not
affected by the changes.

.. ipython:: python
    
    # run our sample pipeline
    dag.build()
    # the status is updated, no need to run again
    dag.status(clear_cached_status=True)


A lot of data work is done interactively using Jupyter or similar tools, being
able interact with a pipeline in the same way is an effective way of
experimenting new methods.

.. ipython:: python
    
    # say you are adding a new method to task make_data, you can run your code
    # with all upstream dependencies being taken care of like this

    # run your task
    dag['make_data'].build(force=True)

    # explore results - reading the file this way guarantees you are using
    # the right file
    df = pd.read_parquet(str(dag['make_data']))
    df


ease debugging

plot and dag.status

.. ipython:: python

    dag['make_data'].product
    # dag['make_data'].source
    dag.status()
    # execution summary


testing


Transition to a production system
*********************************

Once your pipeline is to be taken to production, requirements should be
higher. Data processing pipelines require a lot of experimentation, a progress
is made, more and more code is added and soon, the pipeline becomes a black
box: what resources does it need? where does it store output? where is the SQL
script to pull the data?

Since a DAG is a full specification of your pipeline, it can answer all those
questions:


.. ipython:: python

    dag.status()


From the status, we can see that this pipeline uses a database, we can also see
where the output will go and the location for each task source code.

This simple, yet effective feature makes our pipeline transparent for anyone
looking to productionize our code (e.g. a production engineer) or even a
colleague who just started working on the project.

