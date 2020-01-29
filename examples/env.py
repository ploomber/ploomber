"""
Using ploomber.env to isolate pipeline execution between team members
"""
from pathlib import Path
import tempfile

import pandas as pd

from ploomber import DAG
from ploomber import Env
from ploomber.tasks import PythonCallable
from ploomber.products import File

tmp_dir = Path(tempfile.mkdtemp())

# Env can be used to centralize configuration parameters that can be switched
# between users or servers - this configuration ensures that the output
# location is automatically determined (by using the {{user}} placeholder)
# to each user, see ploomber.Env documentation for more details
env = Env.from_dict({'path': {
    'raw': str(tmp_dir / '{{user}}' / 'raw'),
    'clean': str(tmp_dir / '{{user}}' / 'clean')
    }
})

dag = DAG(name='my pipeline')


def get_wine_data(product, filename):
    """Get red wine data
    """
    url = ('http://archive.ics.uci.edu/ml/machine-learning-databases/'
           f'wine-quality/{filename}')
    df = pd.read_csv(url,
                     sep=';',
                     index_col=False)
    # producg is a File type so you have to cast it to a str
    df.to_csv(str(product))


def concat_data(upstream, product):
    """Concatenate red and white wine data
    """
    red = pd.read_csv(str(upstream['red']))
    white = pd.read_csv(str(upstream['white']))
    df = pd.concat([red, white])
    df.to_csv(str(product))


red_task = PythonCallable(get_wine_data,
                          product=File(env.path.raw / 'red.csv'),
                          dag=dag,
                          name='red',
                          params={'filename': 'winequality-red.csv'})

white_task = PythonCallable(get_wine_data,
                            product=File(env.path.raw / 'white.csv'),
                            dag=dag,
                            name='white',
                            params={'filename': 'winequality-white.csv'})

concat_task = PythonCallable(concat_data,
                             product=File(env.path.clean / 'all.csv'),
                             dag=dag, name='all')


red_task >> concat_task
white_task >> concat_task

dag.build()
