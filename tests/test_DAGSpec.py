from glob import glob
from datetime import timedelta, datetime
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
from pathlib import Path
import pytest
from ploomber.dag import DAGSpec
import yaml
from conftest import _path_to_tests, fixture_tmp_dir
import jupytext
import nbformat
import jupyter_client


@fixture_tmp_dir(_path_to_tests() / 'assets' / 'pipeline-sql')
def tmp_pipeline_sql():
    pass


@fixture_tmp_dir(_path_to_tests() / 'assets' / 'nbs-auto')
def tmp_nbs_auto():
    pass


def to_ipynb(dag_spec):
    for source in ['load.py', 'clean.py', 'plot.py']:
        nb = jupytext.read(source)
        Path(source).unlink()

        k = jupyter_client.kernelspec.get_kernel_spec('python3')

        nb.metadata.kernelspec = {
            "display_name": k.display_name,
            "language": k.language,
            "name": 'python3'
        }

        nbformat.write(nb, source.replace('.py', '.ipynb'))

    for task in dag_spec['tasks']:
        task['source'] = task['source'].replace('.py', '.ipynb')

    return dag_spec


def tasks_list(dag_spec):
    return dag_spec['tasks']


def remove_task_class(dag_spec):
    for task in dag_spec['tasks']:
        del task['class']

    return dag_spec


def infer_upstream(dag_spec):
    dag_spec['meta']['infer_upstream'] = True

    for task in dag_spec['tasks']:
        task.pop('upstream', None)

    return dag_spec


def extract_product(dag_spec):
    dag_spec['meta']['extract_product'] = True

    for task in dag_spec['tasks']:
        task.pop('product', None)

    return dag_spec


@pytest.mark.parametrize('processor', [to_ipynb, tasks_list, remove_task_class,
                                       infer_upstream, extract_product])
def test_notebook_spec(processor, tmp_nbs):
    Path('output').mkdir()

    with open('pipeline.yaml') as f:
        dag_spec = yaml.load(f, Loader=yaml.SafeLoader)

    dag_spec = processor(dag_spec)

    dag = DAGSpec.init_dag(dag_spec)
    dag.build()


def test_notebook_spec_w_location(tmp_nbs, add_current_to_sys_path):

    Path('output').mkdir()

    with open('pipeline-w-location.yaml') as f:
        dag_spec = yaml.load(f, Loader=yaml.SafeLoader)

    dag = DAGSpec.init_dag(dag_spec)
    dag.build()


def test_spec_from_list_of_files(tmp_nbs_auto):
    Path('output').mkdir()
    dag = DAGSpec.init_dag(glob('*.py'))
    dag.build()


def _random_date_from(date, max_days, n):
    return [date + timedelta(days=int(days))
            for days in np.random.randint(0, max_days, n)]


def test_sql_spec(tmp_pipeline_sql):
    with open('pipeline.yaml') as f:
        dag_spec = yaml.load(f, Loader=yaml.SafeLoader)

    dates = _random_date_from(datetime(2016, 1, 1), 365, 100)
    df = pd.DataFrame({'customer_id': np.random.randint(0, 5, 100),
                       'value': np.random.rand(100),
                       'purchase_date': dates})
    engine = create_engine(dag_spec['config']['clients']['SQLScript'])
    df.to_sql('sales', engine)
    engine.dispose()

    dag = DAGSpec.init_dag(dag_spec)

    # FIXME: this does no show the custom Upstream key missing error
    dag.build()

    assert not dag['load.sql'].upstream
    assert list(dag['filter.sql'].upstream.keys()) == ['load.sql']
    assert list(dag['transform.sql'].upstream.keys()) == ['filter.sql']


def test_init_with_tasks_list():
    spec_raw = [{'source': 'load.py', 'product': 'load.ipynb'}]
    spec = DAGSpec.DAGSpec(spec_raw)
    assert spec['meta']['infer_upstream']
    assert spec['tasks'] == spec_raw


def test_infer_upstream_with_empty_meta():
    spec = DAGSpec.DAGSpec(['load.py'])
    assert spec['meta']['infer_upstream']


def test_infer_upstream_with_empty_meta_infer_upstream():
    spec = DAGSpec.DAGSpec({'meta': {'some_key': None}, 'tasks': []})
    assert spec['meta']['infer_upstream']


def test_infer_upstream():
    spec = DAGSpec.DAGSpec({'meta': {'infer_upstream': False}, 'tasks': []})
    assert not spec['meta']['infer_upstream']
