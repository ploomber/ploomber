"""
Tests for dagspecs using the grid feature
"""
from pathlib import Path

import pytest
import yaml
from ploomber.spec import DAGSpec


@pytest.fixture
def write_tasks():
    Path('upstream.py').write_text("""
# + tags=['parameters']
upstream = None
param = None

# +
1 + 1
""")

    Path('downstream.py').write_text("""
# + tags=['parameters']
upstream = ['upstream-*']

# +
1 + 1
""")


# NOTE: test_dagspec.py has the original tests for the grid feature but the
# file is getting too large so we'll add new ones here. We need to move the
# old ones to this file
@pytest.fixture
def sample_dagspec(tmp_directory, write_tasks):
    spec = {
        'tasks': [{
            'source': 'upstream.py',
            'name': 'upstream-',
            'grid_number_suffix': True,
            'product': 'output/param=[[param]].ipynb',
            'grid': {
                'param': [1, 2]
            }
        }, {
            'source': 'downstream.py',
            'product': 'downstream.ipynb'
        }]
    }

    Path('pipeline.yaml').write_text(yaml.dump(spec))


@pytest.fixture
def sample_dagspec_with_placeholder_task_names(tmp_directory, write_tasks):
    spec = {
        'tasks': [{
            'source': 'upstream.py',
            'name': 'upstream-param=[[param]]',
            'product': 'output/param=[[param]].ipynb',
            'grid': {
                'param': [1, 2]
            }
        }, {
            'source': 'downstream.py',
            'product': 'downstream.ipynb'
        }]
    }

    Path('pipeline.yaml').write_text(yaml.dump(spec))


@pytest.fixture
def sample_dagspec_with_params(tmp_directory, write_tasks):
    spec = {
        'tasks': [{
            'source': 'upstream.py',
            'name': 'upstream-',
            'grid_number_suffix': True,
            'product': 'output/param=[[param]].ipynb',
            'grid': {
                'param': [1, 2]
            },
            'params': {
                'another': 100,
                'one-more': 200
            }
        }, {
            'source': 'downstream.py',
            'product': 'downstream.ipynb'
        }]
    }

    Path('pipeline.yaml').write_text(yaml.dump(spec))


@pytest.fixture
def sample_dagspec_with_suffix_off(tmp_directory, write_tasks):
    spec = {
        'tasks': [{
            'source': 'upstream.py',
            'name': 'upstream-[[param]]',
            'grid_number_suffix': False,
            'product': 'output/param.ipynb',
            'grid': {
                'param': [1, 2]
            }
        }, {
            'source': 'downstream.py',
            'product': 'downstream.ipynb'
        }]
    }

    Path('pipeline.yaml').write_text(yaml.dump(spec))


@pytest.fixture
def sample_dagspec_missing_place_holders(tmp_directory, write_tasks):
    spec = {
        'tasks': [{
            'source': 'upstream.py',
            'name': 'upstream-',
            'grid_number_suffix': False,
            'product': 'output/param=[[param]].ipynb',
            'grid': {
                'param': [1, 2],
                'place_holders': [5, 10],
                'random': [4, 3]
            }
        }, {
            'source': 'downstream.py',
            'product': 'downstream.ipynb'
        }]
    }

    Path('pipeline.yaml').write_text(yaml.dump(spec))


@pytest.fixture
def sample_dagspec_with_suffix_on(tmp_directory, write_tasks):
    spec = {
        'tasks': [{
            'source': 'upstream.py',
            'name': 'upstream-',
            'grid_number_suffix': True,
            'product': 'output/param.ipynb',
            'grid': {
                'param': [1, 2]
            }
        }, {
            'source': 'downstream.py',
            'product': 'downstream.ipynb'
        }]
    }

    Path('pipeline.yaml').write_text(yaml.dump(spec))


@pytest.fixture
def sample_dagspec_with_no_flag(tmp_directory, write_tasks):
    spec = {
        'tasks': [{
            'source': 'upstream.py',
            'name': 'upstream-',
            'product': 'output/param=[[param]].ipynb',
            'grid': {
                'param': [1, 2, 3]
            }
        }, {
            'source': 'downstream.py',
            'product': 'downstream.ipynb'
        }]
    }

    Path('pipeline.yaml').write_text(yaml.dump(spec))


@pytest.fixture
def sample_dagspec_with_no_flag_error(tmp_directory, write_tasks):
    spec = {
        'tasks': [{
            'source': 'upstream.py',
            'name': 'upstream-',
            'product': 'output/param.ipynb',
            'grid': {
                'param': [1, 2]
            }
        }, {
            'source': 'downstream.py',
            'product': 'downstream.ipynb'
        }]
    }

    Path('pipeline.yaml').write_text(yaml.dump(spec))


def test_grid_with_params_placeholders(sample_dagspec):
    dag = DAGSpec('pipeline.yaml').to_dag()

    assert 'param=1-0.ipynb' in str(dag['upstream-0'].product)
    assert 'param=2-1.ipynb' in str(dag['upstream-1'].product)


def test_grid_with_params(sample_dagspec_with_params):
    dag = DAGSpec('pipeline.yaml').to_dag()

    assert dag['upstream-0'].params == {
        'param': 1,
        'another': 100,
        'one-more': 200
    }
    assert dag['upstream-1'].params == {
        'param': 2,
        'another': 100,
        'one-more': 200
    }


def test_with_with_placeholder_task_names(
        sample_dagspec_with_placeholder_task_names):
    dag = DAGSpec('pipeline.yaml').to_dag()

    assert 'param=1-0.ipynb' in str(dag['upstream-param=1'].product)
    assert 'param=2-1.ipynb' in str(dag['upstream-param=2'].product)


def test_grid_number_suffix_off(
        sample_dagspec_with_suffix_off):

    with pytest.raises(NameError) as e:
        DAGSpec('pipeline.yaml').to_dag()

    assert 'Unable to resolove pipeline. ' in str(e)


def test_grid_number_suffix_on(
        sample_dagspec_with_suffix_on):
    dag = DAGSpec('pipeline.yaml').to_dag()

    assert 'param-0.ipynb' in str(dag['upstream-0'].product)
    assert 'param-1.ipynb' in str(dag['upstream-1'].product)


def test_grid_number_suffix_missing_placeholders(
        sample_dagspec_missing_place_holders):

    with pytest.raises(NameError) as e:
        DAGSpec('pipeline.yaml').to_dag()

    assert 'Unable to resolove pipeline. ' in str(e)


def test_grid_number_no_flag(
        sample_dagspec_with_no_flag):
    dag = DAGSpec('pipeline.yaml').to_dag()

    assert 'param=1-0.ipynb' in str(dag['upstream-0'].product)
    assert 'param=2-1.ipynb' in str(dag['upstream-1'].product)
    assert 'param=3-2.ipynb' in str(dag['upstream-2'].product)


def test_grid_number_no_flag_error(
        sample_dagspec_with_no_flag_error):

    with pytest.raises(NameError) as e:
        DAGSpec('pipeline.yaml').to_dag()

    assert 'Unable to resolove pipeline. ' in str(e)
