from pathlib import Path

from ploomber import DAG, with_env
from ploomber.tasks import NotebookRunner
from ploomber.products import File


@with_env
def make(env):
    dag = DAG()

    out = Path('output')
    out.mkdir(exist_ok=True)

    # our first task is a Python function, it outputs a csv file
    load = NotebookRunner(Path('load.py'),
                          product={
                              'nb': File(out / 'load.ipynb'),
                              'data': File(out / 'data.csv')
                          },
                          dag=dag,
                          name='load')

    clean = NotebookRunner(
        Path('clean.py'),
        # this task generates two files, the .ipynb
        # output notebook and another csv file
        product={
            'nb': File(out / 'clean.ipynb'),
            'data': File(out / 'clean.csv')
        },
        dag=dag,
        name='clean')

    plot = NotebookRunner(Path('plot.py'),
                          File(out / 'plot.ipynb'),
                          dag=dag,
                          name='plot')

    load >> clean >> plot

    return dag
