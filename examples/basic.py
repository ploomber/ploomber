"""
Basic pipeline
==============

This example shows the most basic usage

Note: to see the plots you'll need to run this in a Jupyter notebook
"""
from pathlib import Path
import tempfile

import pandas as pd
from IPython.display import Image, display

from ploomber import DAG
from ploomber.tasks import PythonCallable
from ploomber.products import File


# A `DAG` is a workflow representation, it is a collection of `Tasks` that
# are executed in a given order, each task is associated with a `Product`,
# which is a persistent change in a system (i.e. a table in a remote database
# or a file in the local filesystem), a task can products from other tasks as
# inputs, these are known as upstream dependencies, finally, a task can have
# extra parameters, but it is recommended to keep these as simple as possible.

# ## Building a simple DAG

dag = DAG(name='my pipeline')


# Let's now build the first tasks, they will just download some data, this
# pipeline is entirely declared in a single file to simplify things, a real
# pipeline will likely be splitted among several files.

# Tasks can be a lot of things (bash scripts, SQL scripts, etc), for this
# example they will be Python functions.

# these function pull the data and save it, the product
# parameter is required in every Task

def get_red_wine_data(product):
    """Get red wine data
    """
    url = ('http://archive.ics.uci.edu/ml/machine-learning-databases/'
           'wine-quality/winequality-red.csv')
    df = pd.read_csv(url,
                     sep=';',
                     index_col=False)
    # producg is a File type so you have to cast it to a str
    df.to_csv(str(product))


def get_white_wine_data(product):
    """Get white wine data
    """
    url = ('http://archive.ics.uci.edu/ml/machine-learning-databases/'
           'wine-quality/winequality-white.csv')
    df = pd.read_csv(url,
                     sep=';',
                     index_col=False)
    df.to_csv(str(product))


# if the task has any dependencies, an upstream parameter is required

def concat_data(upstream, product):
    """Concatenate red and white wine data
    """
    red = pd.read_csv(str(upstream['red']))
    white = pd.read_csv(str(upstream['white']))
    df = pd.concat([red, white])
    df.to_csv(str(product))


# create a temporary directory to store data
tmp_dir = Path(tempfile.mkdtemp())

# convert our functions to Task objects, note
# that the product is a File object, which means
# this functions will create a file in the local filesystem
red_task = PythonCallable(get_red_wine_data,
                          product=File(tmp_dir / 'red.csv'),
                          dag=dag, name='red')

white_task = PythonCallable(get_white_wine_data,
                            product=File(tmp_dir / 'white.csv'),
                            dag=dag, name='white')

concat_task = PythonCallable(concat_data,
                             product=File(tmp_dir / 'all.csv'),
                             dag=dag, name='all')

# now we declare how our tasks relate to each other
red_task >> concat_task
white_task >> concat_task

# we can plot our dag
path_to_image = dag.plot(open_image=False)
display(Image(filename=path_to_image))

# build the dag (execute all the tasks)
dag.build()

# Each time the DAG is run it will save the current timestamp and the
# source code of each task, next time we run it it will only run the
# necessary tasks to get everything up-to-date, there is a simple rule to
# that: a task will run if its code (or the code from any dependency) has
# changed since the last time it ran.


# the plot will show which tasks are up-to-date
# in green
path_to_image = dag.plot(open_image=False, clear_cached_status=True)
display(Image(filename=path_to_image))

# status returns a summary of each task status
dag.status()

# ## Inspecting the `DAG` object

# The DAG object has utilities to debug and use the pipeline.

# list all tasks in the dag
list(dag)

# get a task
task = dag['red']
task

# task plan returns the source code to be executed along with the input
# parameters and product
task.plan()

# avoid hardcoding paths to files by loading them directly
# from the DAG, casting a Task to a str, will cause them
# to return a valid representation, in this case, our
# product is a File, so it will return a path to it, for SQL relations
# str(product) will return "schema"."name". Using this method for loading
# products makes sure you don't have to hardcode paths to files and that
# given your pipeline definition, you always read from the right place
df = pd.read_csv(str(dag['red']))

df.head()
