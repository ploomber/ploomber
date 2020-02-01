"""
Dynamic DAGs
============

How to build DAGs dynamically

Dynamic DAGs are pipelines whose tasks are determined by one or more variables.
This variables affect how many and which code will be included on each task.

One of the most common use cases is for splitting a large dataset into batches,
say you are working with a large dataset where each observation has a
timestamp. You could generate a DAG that given a start and an end date
generates one task per year, so that each one processes one year of data.

Why would you do that? Here are some use cases:
    1. Avoid long-running queries. If you are pulling data from a database
    your query might be killed if it takes too much to run, splitting into
    batches helps you split your data dump in several parts
    2. Process batchs independently (larger than memory). If your dataset
    does not fit into memory and each observation can be processed
    independently, you can split into batches and process each one at a time
    (NOTE: before you try any technique to process larger-than-memory
    datasets, check if you can get away with SQL, databases are designed to
    handle very large datasets without you having to worry about the details)
    3. Recover from crashes. If at any point your code crashes due to external
    circumtances, splitting processing in batches ensures that you will only
    have to run the interrupted batch as opposed to the whole dataset
    4. Get newest data. If your pipeline has to be run on a schedule to ingest
    and process new data, you can create a dynamic DAG that pulls the latest
    (say daily) observations and process them instead of pulling the complete
    historical data every time
"""
from datetime import date
from pathlib import Path
import tempfile
import sqlite3

from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd

from ploomber import DAG
from ploomber.tasks import SQLDump
from ploomber.products import File
from ploomber.clients import SQLAlchemyClient

tmp_dir = Path(tempfile.mkdtemp())
path_to_db = tmp_dir / 'my_db.db'

# first generate some sample data, one daily observation from 2010 to 2020
dates = pd.date_range('2010', '2020', freq='D')
df = pd.DataFrame({'date': dates,
                   'x': np.random.rand(len(dates))})

conn = sqlite3.connect(str(path_to_db))
df.to_sql('data', conn)
conn.close()

dag = DAG()

dag.clients[SQLDump] = SQLAlchemyClient('sqlite:///' + str(path_to_db))


def make_task(date_start, date_end, path, dag):
    """Task factory: returns a task that dumps certain year
    """
    sql = """
    SELECT * FROM data
    WHERE DATE('{{date_start}}') <= date AND date < DATE('{{date_end}}')
    """
    name = f'{date_start}-to-{date_end}.csv'
    return SQLDump(sql,
                   product=File(Path(path / name)),
                   dag=dag,
                   name=f'dump_{name}',
                   params={'date_start': date_start, 'date_end': date_end},
                   chunksize=None)


# generate (date_start, date_end) pairs
dates = [(date(2010, 1, 1) + relativedelta(years=i),
          date(2010, 1, 1) + relativedelta(years=i + 1)) for i in range(10)]


# run the task factory for each date pair
for date_start, date_end in dates:
    make_task(date_start, date_end, tmp_dir, dag)


# execute pipeline
dag.build()
