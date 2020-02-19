"""
Debugging Python callables
==========================

ploomber integrates with The Python Debugger (pdb module) to allow interactive
debugging. Since a DAG object has all information it needs to re-create
what happens when a Task is executed, this integration happens transparently.
"""
from pathlib import Path
import tempfile

import pandas as pd

from ploomber import DAG
from ploomber.products import File
from ploomber.tasks import PythonCallable

###############################################################################
# Create a temporary directory
tmp_dir = Path(tempfile.mkdtemp())

###############################################################################
# Now, let's setup our pipeline

dag = DAG()


def _generate_data(product):
    """Add one to column a
    """
    df = pd.DataFrame({'a': [1, 2, 3, 4, 5]})
    df.to_csv(str(product), index=False)


def _transform_data(upstream, product):
    df = pd.read_csv(str(upstream['gen_data']))
    df['b'] = df['a'] + 42
    df['c'] = df['a'] + df['z']
    df.to_csv(str(product), index=False)


generate = PythonCallable(_generate_data,
                          File(tmp_dir / 'gen_data.csv'),
                          dag,
                          name='gen_data')


transform = PythonCallable(_transform_data,
                           File(tmp_dir / 'transformed_data.csv'),
                           dag,
                           name='transformed_data')

generate >> transform


try:
    dag.build()
except Exception as e:
    print('DAG execution failed: ', e)


dag['transformed_data'].debug()
