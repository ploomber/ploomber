"""
How to build DAGs dynamically

Dynamic DAGs let you reduce boilerplate code and reduce computation time for
pipelines that execute on a schedule
"""
from datetime import date
from pathlib import Path
import tempfile
import sqlite3

from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd

from ploomber import DAG
from ploomber.tasks import PythonCallable, SQLDump
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


dates = [(date(2010, 1, 1) + relativedelta(years=i),
          date(2010, 1, 1) + relativedelta(years=i + 1)) for i in range(10)]


for date_start, date_end in dates:
    make_task(date_start, date_end, tmp_dir, dag)


dag.build()
