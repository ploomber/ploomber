"""
Basic pipeline
==============

We illustrate ploomber's core features with a typical data pipeline scenario:
dump data from a database and apply a transformation to it. We use sqlite3 for
this example but ploomber supports any database supported by sqlalchemy
without code changes.
"""
import sqlite3
from pathlib import Path
import tempfile

import pandas as pd
from sqlalchemy import create_engine

from ploomber import DAG
from ploomber.products import File
from ploomber.tasks import PythonCallable, SQLDump
from ploomber.clients import SQLAlchemyClient

###############################################################################
# This first part just exports some sample data to a database
tmp_dir = Path(tempfile.mkdtemp())
uri = 'sqlite:///' + str(tmp_dir / 'example.db')
engine = create_engine(uri)
df = pd.DataFrame({'a': [1, 2, 3, 4, 5]})
df.to_sql('example', engine)


###############################################################################
# A `DAG` is a workflow representation, it is a collection of `Tasks` that
# are executed in a given order, each task is associated with a `Product`,
# which is a persistent change in a system (i.e. a table in a remote database
# or a file in the local filesystem), a task can products from other tasks as
# inputs, these are known as upstream dependencies, finally, a task can have
# extra parameters, but it is recommended to keep these as simple as possible.
#
# There are three core concepts in ``ploomber``: :class:`Tasks <ploomber.tasks>`,
# :class:`Products <ploomber.products>` and :class:`DAGs <ploomber.DAG>`. Tasks
# are units of work that generate Products, Tasks dependencies are organized in
# a DAG.


dag = DAG()

# the first task dumps data from the db to the local filesystem
task_dump = SQLDump('SELECT * FROM example',
                    File(tmp_dir / 'example.csv'),
                    dag,
                    name='dump',
                    client=SQLAlchemyClient(uri),
                    chunksize=None)


# we will add one to the data
# if the task has any dependencies, an upstream parameter is required
def _add_one(upstream, product):
    df = pd.read_csv(str(upstream['dump']))
    df['a'] = df['a'] + 1
    df.to_csv(str(product), index=False)


# we convert the Python function to a Task
task_add_one = PythonCallable(_add_one,
                              File(tmp_dir / 'add_one.csv'),
                              dag,
                              name='add_one')

# declare how tasks relate to each other
task_dump >> task_add_one




# the plot will show which tasks are up-to-date
# in green
dag.plot(output='matplotlib', clear_cached_status=True)

###############################################################################
# Inspecting a pipeline
# *********************

# A lot of data pipelines start as experimental projects (e.g. developing a 
# Machine Learning model), which causes them to grow unpredictably. As the
# pipeline evolves, it will span dozens of files whose intent is unclear. The
# DAG object serves as the primary reference for anyone seeking to understand
# the pipeline.


# Making a pipeline transparent helps others quickly understand it without going
# through the code details and eases debugging for developers.
# status returns a summary of each task status
dag.status()


###############################################################################
# Each time the DAG is run it will save the current timestamp and the
# source code of each task, next time we run it it will only run the
# necessary tasks to get everything up-to-date, there is a simple rule to
# that: a task will run if its code (or the code from any dependency) has
# changed since the last time it ran.

# Data processing pipelines consist on many small long-running tasks which
# depend on each other. During early development phases things are expected to
# change: new tasks are added, bugs are fixed. Triggering a full end-to-end
# run on each change is wasteful. On a successful run, ploomber saves the task
# source code, if the pipeline is run again, it will skip tasks that are not
# affected by the changes.

# run our sample pipeline
dag.build()


# the pipeline is up-to-date, no need to run again
dag.build(clear_cached_status=True)


###############################################################################
# Inspecting the `DAG` object
# ---------------------------
# A lot of data work is done interactively using Jupyter or similar tools, being
# able interact with a pipeline in the same way is an effective way of
# experimenting new methods.

# say you are adding a new method to task add_one, you can run your code
# with all upstream dependencies being taken care of like this

# run your task
dag['add_one'].build(force=True)


###############################################################################
# avoid hardcoding paths to files by loading them directly
# from the DAG, casting a Task to a str, will cause them
# to return a valid representation, in this case, our
# product is a File, so it will return a path to it, for SQL relations
# str(product) will return "schema"."name". Using this method for loading
# products makes sure you don't have to hardcode paths to files and that
# given your pipeline definition, you always read from the right place


# explore results - reading the file this way guarantees you are using
# the right file
df = pd.read_csv(str(dag['add_one']))
df

