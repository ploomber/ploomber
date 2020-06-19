from datetime import timedelta, datetime
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
from pathlib import Path
import pytest
from ploomber.dag import DAGSpec
import yaml
from conftest import _path_to_tests, fixture_tmp_dir


@fixture_tmp_dir(_path_to_tests() / 'assets' / 'pipeline-sql')
def tmp_pipeline_sql():
    pass


@pytest.mark.parametrize('spec', ['pipeline.yaml',
                                  'pipeline-w-location.yaml'])
def test_notebook_spec(spec, tmp_nbs, add_current_to_sys_path):

    Path('output').mkdir()

    with open(spec) as f:
        dag_spec = yaml.load(f, Loader=yaml.SafeLoader)

    dag = DAGSpec.init_dag(dag_spec)
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
