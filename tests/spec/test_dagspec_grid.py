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
def sample_dagspec_with_default_params(tmp_directory, write_tasks):
    spec = {
        'task_defaults': {
            'NotebookRunner': {
                'check_if_kernel_installed': False
            }
        },
        'tasks': [{
            'source': 'upstream.py',
            'name': 'upstream-',
            'product': 'output/param=[[param]].ipynb',
            'grid': {
                'param': [1, 2]
            },
            'params': {
                'another': 100,
                'one-more': 200
            }
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


def test_with_placeholder_task_names(
        sample_dagspec_with_placeholder_task_names):
    dag = DAGSpec('pipeline.yaml').to_dag()

    assert 'param=1-0.ipynb' in str(dag['upstream-param=1'].product)
    assert 'param=2-1.ipynb' in str(dag['upstream-param=2'].product)


def test_default_parameter_grid(
        sample_dagspec_with_default_params):
    dag = DAGSpec('pipeline.yaml').to_dag()

    assert dag['upstream-0'].params == {
        'param': 1,
        'check_if_kernel_installed': False,
        'another': 100,
        'one-more': 200
    }
    assert dag['upstream-0'].check_if_kernel_installed is False
    assert dag['upstream-1'].check_if_kernel_installed is False
