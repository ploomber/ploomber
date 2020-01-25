Introduction
============

Goals
-----

ploomber's goals are to accelerate experimentation and facilitate building
production systems. It achieves these goals by providing incremental
builds, interactive execution, reducing boilerplate code, tools to inspect
a pipeline object and facilitate testing.


Example
-------

There are three core concepts in ``ploomber``: :class:`Tasks <ploomber.tasks>`,
:class:`Products <ploomber.products>` and :class:`DAGs <ploomber.DAG>`. Tasks
are units of work that generate Products, Tasks dependencies are organized in
a DAG.

We will illustrate ploomber's core feature through an example that features a
common data pipeline scenario: dump data from a database and apply
atransformation to it. We will use  sqlite3 for this example but ploomber
supports any database supported by sqlalchemy without code changes.

This first part creates a database and populates a table:

.. ipython:: python
    
    import tempfile
    import sqlite3
    from pathlib import Path

    from sqlalchemy import create_engine
    import pandas as pd

    tmp_dir = Path(tempfile.mkdtemp())
    uri = 'sqlite:///' + str(tmp_dir / 'example.db')
    engine = create_engine(uri)
    df = pd.DataFrame({'a': [1, 2, 3, 4, 5]})
    df.to_sql('example', engine)


Let's now define the actual pipeline:

.. ipython:: python

    from ploomber import DAG
    from ploomber.products import File
    from ploomber.tasks import PythonCallable, SQLDump
    from ploomber.clients import SQLAlchemyClient

    dag = DAG()

    # the first task dumps data from the db to the local filesystem
    task_dump = SQLDump('SELECT * FROM example',
                        File(tmp_dir / 'example.csv'),
                        dag,
                        name='dump',
                        client=SQLAlchemyClient(uri),
                        chunksize=None)

.. ipython:: python
    
    # we will add one to the data
    def _add_one(upstream, product):
        df = pd.read_csv(str(upstream['dump']))
        df['a'] = df['a'] + 1
        df.to_csv(str(product), index=False)


.. ipython:: python
    
    # we convert the Python function to a Task
    task_add_one = PythonCallable(_add_one,
                                  File(tmp_dir / 'add_one.csv'),
                                  dag,
                                  name='add_one')

    # declare how tasks relate to each other
    task_dump >> task_add_one


Incremental builds
******************

Data processing pipelines consist on many small long-running tasks which
depend on each other. During early development phases things are expected to
change: new tasks are added, bugs are fixed. Triggering a full end-to-end
run on each change is wasteful. On a successful run, ploomber saves the task
source code, if the pipeline is run again, it will skip tasks that are not
affected by the changes.

.. ipython:: python
    
    # run our sample pipeline
    dag.build()
    # the pipeline is up-to-date, no need to run again
    dag.build(clear_cached_status=True)


Interactive execution
*********************

A lot of data work is done interactively using Jupyter or similar tools, being
able interact with a pipeline in the same way is an effective way of
experimenting new methods.

.. ipython:: python
    
    # say you are adding a new method to task add_one, you can run your code
    # with all upstream dependencies being taken care of like this

    # run your task
    dag['add_one'].build(force=True)

    # explore results - reading the file this way guarantees you are using
    # the right file
    df = pd.read_csv(str(dag['add_one']))
    df


Reduce boilerplate code
***********************

Most data pipelines involve interacting with external resources (e.g. a remote
database) or local processes (e.g. run an R script). Interacting with external
resources or processes requires adding supporting code to manage them, if not
managed properly, this code gets in the way of the relevant code (the one that
performs data transformations) and offuscates their intent, adding a cognitive
overhead for whoever is reading the code.

[ADD CODE EXAMPLE]

Inspecting a pipeline
*********************

A lot of data pipelines start as experimental projects (e.g. developing a 
Machine Learning model), which causes them to grow unpredictably. As the
pipeline evolves, it will span dozens of files whose intent is unclear. The
DAG object serves as the primary reference for anyone seeking to understand
the pipeline.

.. ipython:: python
    
    # status provides a summary of the pipeline state, dag.plot() plots the
    # pipeline
    dag.status()

Making a pipeline transparent helps others quickly understand it without going
through the code details and eases debugging for developers.

Testing
*******

Testing is crucial for any data pipeline. Raw data comes from sources where
developers have no control over. Raw data inconsistencies are a common source
for bugs in data pipelines (e.g. a unusual high proportion of NAs in certain
column), but to make progress, a developer must make assumptions about the
input data and such assumptions must be tested every time the pipeline runs
to prevent errors from propagating to downstream tasks. Let's rewrite our
previous pipeline to implement this defensive programming approach:

.. ipython:: python
    
    def check_no_nas(task):
        df = pd.read_csv(str(task))
        assert not df['a'].isna().sum()


.. ipython:: python

    # run check_no_nas after task_add_one finishes
    task_add_one.on_finish = check_no_nas
    dag.build(force=True)

Your DAG is a contract
----------------------

Once your pipeline is to be taken to production, it should be expected to run
in a predictable and reliable way. When your code is taken to a production
environment, it will be natural to ask questions such as: what resources does
it need? where does it store output? where is the SQL script to pull the data?

Answers to these questions could be provided in documentation, but if anything
changes, the documentation most likely will become outdated, useless in the
best case, confusing in the worst.

Since a DAG object is a full specification of your pipeline, it can answer all those questions. Taking a look at our pipeline specification we can clearly see
that our pipeline uses a sqlite database, stores its output in a temporary
directory and the query to pull the data. Furthermore, the status method
provides a summary of the pipeline's current state:

.. ipython:: python

    dag.status()


The ``dag`` object should be treated as a contract and developers must adhere
to it. This simple, yet effective feature makes our pipeline transparent for
anyone looking to productionize our code (e.g. a production engineer) or even
a colleague who just started working on the project.

What? Another workflow management tool?
---------------------------------------

Before starting this project, I wondered whether there would be already a tool
to cover my needs and found none. I already knew about Airflow (where ploomber
takes a lot of inspiration from), but when going through the documentation I found out it was not aligned with my goals and the setup seemed an overkill.

The closest project I could find in terms of goals was
`drake <https://github.com/ropensci/drake>`_, but 1) is an R tool and 2) lacked
of some features I consider critical for production systems.

As far as I know, there are no workflow management tools that make the notion
of products explicit, from my experience, this is a huge source of bugs: each
developer opens connections to their own resources and saves to external
systems in each task, which leads to unrealiable pipelines that break when
deployed in a new system. `Metaflow <https://metaflow.org/>`_ completely
removes control over products by automatically serializing all variable
on each task, but that seems an overabstraction to me.

Furthermore, a lot of tools bundle tons of extra features that are not needed
in early experimentation phases which greatly increases setup time and requires
users to go through long documentation. I believe some of these features
(such as scheduling) should be treated as separate problems, bundling them
together locks your project to specific frameworks. ploomber aims to provide
only the most important features and let important implementation details up
to the developer.

Fianlly, some tools add so much boilerplate code that for someone unfamiliar
with the tool, it will be hard to understand which parts are tool-specific
and which ones are the relevant data transformation parts. Production systems
often have strict requirements and ploomber aims to be as transparent as
possible: relevant code should be clear and should run correctly without
ploomber with the least amount of changes.
