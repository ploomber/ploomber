"""
Parametrized DAGs
=================

How to parametrize Tasks to reduce boilerplate code
"""
from pathlib import Path
import tempfile

import pandas as pd

from ploomber import DAG
from ploomber.tasks import PythonCallable, SQLUpload, SQLScript
from ploomber.products import File, SQLiteRelation
from ploomber.clients import SQLAlchemyClient


tmp_dir = Path(tempfile.mkdtemp())
path_to_db = 'sqlite:///' + str(tmp_dir / 'my_db.db')
print('temporary dir: ', tmp_dir)

dag = DAG()

client = SQLAlchemyClient(path_to_db)
dag.clients[SQLUpload] = client
dag.clients[SQLiteRelation] = client
dag.clients[SQLScript] = client


def get_data(product, filename):
    """Get red wine data
    """
    url = ('http://archive.ics.uci.edu/ml/machine-learning-databases/'
           'wine-quality/' + filename)
    df = pd.read_csv(url,
                     sep=';', index_col=False)
    df.to_parquet(str(product))


def concat_data(upstream, product):
    """Concatenate red and white wine data
    """
    red = pd.read_parquet(str(upstream['red']))
    red['kind'] = 'red'
    white = pd.read_parquet(str(upstream['white']))
    white['kind'] = 'white'
    df = pd.concat([red, white])
    df.to_parquet(str(product))


###############################################################################
# in both red_task and white_task, we use the same function get_data,
# but pass different parameters
red_task = PythonCallable(get_data,
                          product=File(tmp_dir / 'red.parquet'),
                          dag=dag,
                          name='red',
                          params={'filename': 'winequality-red.csv'})

white_task = PythonCallable(get_data,
                            product=File(tmp_dir / 'white.parquet'),
                            dag=dag,
                            name='white',
                            params={'filename': 'winequality-white.csv'})

concat_task = PythonCallable(concat_data,
                             product=File(tmp_dir / 'all.parquet'),
                             dag=dag, name='all')


upload_task = SQLUpload(tmp_dir / 'all.parquet',
                        product=SQLiteRelation((None, 'data', 'table')),
                        dag=dag,
                        name='upload')


###############################################################################
# you can use jinja2 to parametrize SQL, {{upstream}} and {{product}}
# are available for your script. this way you could switch products without
# changing your source code (e.g. each Data Scientist in your team writes
# to his/her own db schema to have isolated runs)
sql = """
CREATE TABLE {{product}} AS
SELECT *,
       pH > AVG(pH) AS high_pH
FROM {{upstream['upload']}}
"""

features = SQLScript(sql,
                     product=SQLiteRelation((None, 'features', 'table')),
                     dag=dag,
                     name='features')


red_task >> concat_task
white_task >> concat_task

concat_task >> upload_task >> features

###############################################################################
# render will pass all parameters so you can see exactly which SQL code
# will be executed
dag.render()

###############################################################################
# print source code for task "features"
print(dag['features'].source_code)


dag.plot(output='matplotlib')

dag.build()
