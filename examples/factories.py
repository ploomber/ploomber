"""
Factories
=========

Non-trivial pipelines are expected to span multiple files to improve code
organization and readability. To avoid global mutable state, those files should
expose funcions that return task instances (task factories) that then will be
called by a DAG factory to instantiate a DAG.
"""
from pathlib import Path
import tempfile

import pandas as pd
from sklearn import datasets

from ploomber import DAG, Env, load_env
from ploomber.clients import SQLAlchemyClient
from ploomber.tasks import SQLUpload, PythonCallable, NotebookRunner
from ploomber.products import SQLiteRelation, File

###############################################################################
# A common scenario for Data Science teams is to share computational
# resources, say the team is sharing an analytical database so all
# computations will happen there, for this example we will be using a SQLite
# database but this code could also work in PostgreSQL by just switching
# the product. Also assume that the team shares a big server for Python
# computations

tmp_dir = Path(tempfile.mkdtemp())
db_uri = 'sqlite:///' + str(tmp_dir / 'my_db.db')

# env = Env.set({'path': {'data': '~/data/{{user}}'}}, defaults={'db': 'uri'})

###############################################################################
# Assume the raw data is stored in a remote filesystem so we have to fetch
# it to the shared server first. Assume that the raw data is so big that we do
# not want to save multiple copies, so all team members will read the same raw
# data file, however, we want them to have separate copies or the final,
# so we make it dependent on their user by using the {{user}} placeholder,
# we also want each member to have separate copies of the processed dataset
# so each one will write to a different schema

# NOTE: adding trailing / so Env detects them as directories and creates them
env = Env.from_dict({'path': {'raw': str(tmp_dir / 'raw') + '/',
                              'report': str(tmp_dir / '{{user}}/') + '/'},
                     'schema': '{{user}}', 'db_uri': db_uri})


"""
# Do not do this!

env = Env()

def make_dump():
    some_parameter = env.some_parameter
"""


def _dump(product):
    d = datasets.load_iris()

    df = pd.DataFrame(d['data'])
    df.columns = d['feature_names']
    df['target'] = d['target']
    df['target'] = (df
                    .target.replace({i: name for i, name
                                     in enumerate(d.target_names)}))

    df.to_parquet(str(product))


@load_env
def make_task_dump(env, dag):
    return PythonCallable(_dump,
                          product=File(env.path.raw / 'raw.parquet'),
                          dag=dag,
                          name='raw')


@load_env
def make_task_upload(env, dag):
    return SQLUpload('{{upstream["raw"]}}',
                     product=SQLiteRelation((None, 'raw', 'table')),
                     dag=dag,
                     name='upload')

# TODO: add processed dataset task, use env.schema

@load_env
def make_task_report(env, dag):
    return NotebookRunner()

###############################################################################
# This introduces a subtle but important distinction between Env and Task.params,
# Env is a read-only object for storing rarely changing configuration parameters
# (such as db URIs) where as Task.params are intended to customize a DAG
# behavior, for example, if you want to generate two reports for 2018 and 2019
# data, you can call the a DAG factory twice (both DAGs with the same Env)
# but one dag with the 2018 parameter and another one with the 2019 parameter


@load_env
def make_dag(env, params):
    dag = DAG()
    dag.clients[SQLUpload] = SQLAlchemyClient(env.db_uri)
    dag.clients[SQLiteRelation] = SQLAlchemyClient(env.db_uri)
    dump = make_task_dump(dag)
    transform = make_task_upload(dag)
    dump >> transform
    return dag


###############################################################################
# Now multiple dag objects can be created easily. Note that although DAGs
# (and their tasks) are different objects, they still read from the same Env
# switching Envs in the same process is severely discouraged, if you need to
# run the same pipeline with different Envs, it is better to run them in
# different processes

params_all = [{'kind': 'setosa'}, {'kind': 'virginica'}]

dags = [make_dag(params) for params in params_all]

dags[0].status()

dags[1].status()

for dag in dags:
    dag.build()
