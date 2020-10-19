"""
Acceptance tests
================

Testing is crucial for any data pipeline. Raw data comes from sources where
developers have no control over. Raw data inconsistencies are a common source
for bugs in data pipelines (e.g. a unusual high proportion of NAs in certain
column), but to make progress, a developer must make assumptions about the
input data and such assumptions must be tested every time the pipeline runs
to prevent errors from propagating to downstream tasks. Let's rewrite our
previous pipeline to implement this defensive programming approach:
"""
from pathlib import Path
import tempfile

import pandas as pd
from sqlalchemy import create_engine

from ploomber import DAG
from ploomber.products import File
from ploomber.tasks import PythonCallable, SQLDump
from ploomber.clients import SQLAlchemyClient

###############################################################################
# Setup
tmp_dir = Path(tempfile.mkdtemp())
uri = 'sqlite:///' + str(tmp_dir / 'example.db')
engine = create_engine(uri)
df = pd.DataFrame({'a': [1, 2, 3, 4, 5]})
df.to_sql('example', engine)

###############################################################################
# Pipeline declaration
# ---------------------

dag = DAG()

# the first task dumps data from the db to the local filesystem
task_dump = SQLDump('SELECT * FROM example',
                    File(tmp_dir / 'example.csv'),
                    dag,
                    name='dump',
                    client=SQLAlchemyClient(uri),
                    chunksize=None)


# since this task will have an upstream dependency, it has to accept the
# upstream parameter, all tasks must accept a product parameter
def _add_one(upstream, product):
    """Add one to column a
    """
    df = pd.read_csv(str(upstream['dump']))
    df['a'] = df['a'] + 1
    df.to_csv(str(product), index=False)


def check_a_has_no_nas(task):
    df = pd.read_csv(str(task.product))
    # this print is just here to show that the hook is executed
    print('\n\nRunning on_finish hook: checking column has no NAs...\n\n')
    assert not df.a.isna().sum()


task_add_one = PythonCallable(_add_one,
                              File(tmp_dir / 'add_one.csv'),
                              dag,
                              name='add_one')
task_add_one.on_finish = check_a_has_no_nas

task_dump >> task_add_one

###############################################################################
# Pipeline plot
# -------------
dag.plot()

###############################################################################
# Pipeline build
# -------------
dag.build()
