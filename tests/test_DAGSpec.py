from glob import glob
from datetime import timedelta, datetime
import numpy as np
import pandas as pd
from pathlib import Path
import pytest
from ploomber.spec.DAGSpec import DAGSpec
from ploomber.util.util import _load_factory
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


@fixture_tmp_dir(_path_to_tests() / 'assets' / 'nbs-nested')
def tmp_nbs_nested():
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
    tasks = dag_spec['tasks']

    # we have to pop this, since a list of tasks gets meta default params
    for t in tasks:
        t.pop('upstream', None)
    return tasks


def remove_task_class(dag_spec):
    for task in dag_spec['tasks']:
        del task['class']

    return dag_spec


def extract_upstream(dag_spec):
    dag_spec['meta']['extract_upstream'] = True

    for task in dag_spec['tasks']:
        task.pop('upstream', None)

    return dag_spec


def extract_product(dag_spec):
    dag_spec['meta']['extract_product'] = True

    for task in dag_spec['tasks']:
        task.pop('product', None)

    return dag_spec


def test_validate_top_level_keys():
    with pytest.raises(KeyError):
        DAGSpec({'invalid_key': None})


def test_validate_meta_keys():
    with pytest.raises(KeyError):
        DAGSpec({'tasks': [], 'meta': {'invalid_key': None}})


@pytest.mark.parametrize('processor', [to_ipynb, tasks_list, remove_task_class,
                                       extract_upstream, extract_product])
def test_notebook_spec(processor, tmp_nbs):
    Path('output').mkdir()

    with open('pipeline.yaml') as f:
        dag_spec = yaml.load(f, Loader=yaml.SafeLoader)

    dag_spec = processor(dag_spec)

    dag = DAGSpec(dag_spec).to_dag()
    dag.build()


def test_notebook_spec_nested(tmp_nbs_nested):
    Path('output').mkdir()
    dag = DAGSpec.from_file('pipeline.yaml').to_dag()
    dag.build()


def test_notebook_spec_w_location(tmp_nbs, add_current_to_sys_path):

    Path('output').mkdir()

    with open('pipeline-w-location.yaml') as f:
        dag_spec = yaml.load(f, Loader=yaml.SafeLoader)

    dag = DAGSpec(dag_spec).to_dag()
    dag.build()


@pytest.mark.skip(reason="Won't work until we make extract_product=True the default")
def test_spec_from_list_of_files(tmp_nbs_auto):
    Path('output').mkdir()
    dag = DAGSpec(glob('*.py')).to_dag()
    dag.build()


def _random_date_from(date, max_days, n):
    return [date + timedelta(days=int(days))
            for days in np.random.randint(0, max_days, n)]


def test_postgres_sql_spec(tmp_pipeline_sql, pg_client_and_schema,
                           add_current_to_sys_path):
    with open('pipeline-postgres.yaml') as f:
        dag_spec = yaml.load(f, Loader=yaml.SafeLoader)

    dates = _random_date_from(datetime(2016, 1, 1), 365, 100)
    df = pd.DataFrame({'customer_id': np.random.randint(0, 5, 100),
                       'value': np.random.rand(100),
                       'purchase_date': dates})
    loader = _load_factory(dag_spec['clients']['SQLScript'])
    client = loader()
    df.to_sql('sales', client.engine, if_exists='replace')
    client.engine.dispose()

    dag = DAGSpec(dag_spec).to_dag()

    # FIXME: this does no show the custom Upstream key missing error
    dag.build()

    assert not dag['load.sql'].upstream
    assert list(dag['filter.sql'].upstream.keys()) == ['load.sql']
    assert list(dag['transform.sql'].upstream.keys()) == ['filter.sql']


@pytest.mark.parametrize('spec', ['pipeline-sqlite.yaml',
                                  'pipeline-sqlrelation.yaml'])
def test_sqlite_sql_spec(spec, tmp_pipeline_sql, add_current_to_sys_path):
    with open(spec) as f:
        dag_spec = yaml.load(f, Loader=yaml.SafeLoader)

    dates = _random_date_from(datetime(2016, 1, 1), 365, 100)
    df = pd.DataFrame({'customer_id': np.random.randint(0, 5, 100),
                       'value': np.random.rand(100),
                       'purchase_date': dates})
    loader = _load_factory(dag_spec['clients']['SQLScript'])
    client = loader()
    df.to_sql('sales', client.engine)
    client.engine.dispose()

    dag = DAGSpec(dag_spec).to_dag()

    # FIXME: this does no show the custom Upstream key missing error
    dag.build()

    assert not dag['load.sql'].upstream
    assert list(dag['filter.sql'].upstream.keys()) == ['load.sql']
    assert list(dag['transform.sql'].upstream.keys()) == ['filter.sql']


def test_mixed_db_sql_spec(tmp_pipeline_sql, add_current_to_sys_path,
                           pg_client_and_schema):
    with open('pipeline-multiple-dbs.yaml') as f:
        dag_spec = yaml.load(f, Loader=yaml.SafeLoader)

    dates = _random_date_from(datetime(2016, 1, 1), 365, 100)
    df = pd.DataFrame({'customer_id': np.random.randint(0, 5, 100),
                       'value': np.random.rand(100),
                       'purchase_date': dates})
    # make sales data for pg and sqlite
    loader = _load_factory(dag_spec['clients']['PostgresRelation'])
    client = loader()
    df.to_sql('sales', client.engine, if_exists='replace')
    client.engine.dispose()

    # make sales data for pg and sqlite
    loader = _load_factory(dag_spec['clients']['SQLiteRelation'])
    client = loader()
    df.to_sql('sales', client.engine)
    client.engine.dispose()

    dag = DAGSpec(dag_spec).to_dag()

    # FIXME: this does no show the custom Upstream key missing error
    dag.build()


def test_init_with_tasks_list():
    spec_raw = [{'source': 'load.py', 'product': 'load.ipynb'}]
    spec = DAGSpec(spec_raw)
    assert spec['meta']['extract_upstream']
    assert spec['tasks'] == spec_raw


def test_extract_upstream_with_empty_meta():
    spec = DAGSpec(['load.py'])
    assert spec['meta']['extract_upstream']


def test_extract_upstream_with_empty_meta_extract_upstream():
    spec = DAGSpec({'meta': {}, 'tasks': []})
    assert spec['meta']['extract_upstream']


def test_extract_upstream():
    spec = DAGSpec({'meta': {'extract_upstream': False}, 'tasks': []})
    assert not spec['meta']['extract_upstream']
