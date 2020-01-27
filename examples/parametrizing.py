"""
How to parametrize Tasks to reduce boilerplate code
"""
from pathlib import Path
import tempfile

import pandas as pd

from ploomber import DAG
from ploomber.tasks import PythonCallable
from ploomber.products import File


dag = DAG()


def get_data(product, filename):
    """Get red wine data
    """
    url = ('http://archive.ics.uci.edu/ml/machine-learning-databases/'
           'wine-quality/' + filename)
    df = pd.read_csv(url,
                     sep=';', index_col=False)
    # producg is a File type so you have to cast it to a str
    df.to_csv(str(product))


def concat_data(upstream, product):
    """Concatenate red and white wine data
    """
    red = pd.read_csv(str(upstream['red']))
    white = pd.read_csv(str(upstream['white']))
    df = pd.concat([red, white])
    df.to_csv(str(product))


tmp_dir = Path(tempfile.mkdtemp())
print('temporary dir: ', tmp_dir)

# in both red_task and white_task, we use the same function get_data,
# but pass different parameters
red_task = PythonCallable(get_data,
                          product=File(tmp_dir / 'red.csv'),
                          dag=dag,
                          name='red',
                          params={'filename': 'winequality-red.csv'})

white_task = PythonCallable(get_data,
                            product=File(tmp_dir / 'white.csv'),
                            dag=dag,
                            name='white',
                            params={'filename': 'winequality-white.csv'})

concat_task = PythonCallable(concat_data,
                             product=File(tmp_dir / 'all.csv'),
                             dag=dag, name='all')

red_task >> concat_task
white_task >> concat_task


dag.build()
