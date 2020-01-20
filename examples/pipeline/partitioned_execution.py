"""
Example of parallel processing using partitioned datasets
"""
import time
import pyarrow as pa
from pyarrow import parquet as pq
from scipy import stats
import numpy as np
import pandas as pd

from pathlib import Path

from ploomber import DAG
from ploomber.tasks import PythonCallable
from ploomber.products import File
from ploomber.helpers import partitioned_execution

dag = DAG(executor='parallel')


def _make(product):
    df = pd.DataFrame({'x': np.random.normal(0, 1, size=1000000)})
    df['partition'] = (df.index % 4).astype(int)
    df['group'] = (df.index % 2).astype(int)

    table = pa.Table.from_pandas(df)

    pq.write_to_dataset(table, str(product), partition_cols=['partition'])


def _offset(product):
    df = pd.DataFrame({'offset': np.ones(1000000)})
    df.to_parquet(str(product))


# TODO: document why we need upstream_key here
def _process(upstream, product, upstream_key):
    time.sleep(5)
    df = pd.read_parquet(str(upstream[upstream_key]))
    offset = pd.read_parquet(str(upstream['offset']))
    df['x'] = df['x'] + offset['offset']

    pvalues = df.groupby('group').x.apply(lambda x: stats.normaltest(x)[1])
    pvalues = pd.DataFrame({'pvalue': pvalues})
    pvalues.to_parquet(str(product))


make = PythonCallable(_make, File('observations'), dag,
                      name='make_data')

offset = PythonCallable(_offset, File('offset.parquet'), dag,
                        name='offset')


# partitioned_execution should also accept dags as first parameter, this will
# enable easy execution for machines with small RAM (basically a for loop
# that runs the pipeline over all partitions)
partitioned_execution(make, _process,
                      downstream_prefix='normaltest',
                      downstream_path='results',
                      partition_ids=range(4),
                      partition_template='partition={{id}}',
                      upstream_other=offset)

# TODO: abstract this, should be create when the first partitioned execution
# task is done
Path('results').mkdir(exist_ok=True)

# dag.plot()

dag.build(force=True)

# UPDATE: upstream inside task.params is indeed the product (not the task)
# I forgot I made this change, it happens in Task._render_product,
# upstream in self.params only passes the product, have to clean up the
# rendering flow in Task, is confusing

# I have to clean up the task execution workflow, but works like this:
# - dag.render() makes sure all templated parameters are expanded
# - task.build() is executed on each task
# - some conditions are verified and if needed, the task actually runs via task.run()
# the logic has to come right before task.run() is executed.. but this won't
# work with tasks that need to render params in the source code like SQL
