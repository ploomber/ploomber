import sys
import os
from unittest.mock import Mock, call
from pathlib import Path
import datetime

import pytest

from ploomber_core.telemetry import telemetry
from ploomber.cli import plot, install, build, interact, task, report, status
import ploomber.dag.dag as dag_module
from ploomber.tasks.tasks import PythonCallable
from ploomber.products.file import File
from ploomber_core.exceptions import BaseException
from ploomber.spec import DAGSpec
from ploomber import __version__ as ver

from conftest import _write_sample_conda_env, _prepare_files


@pytest.fixture
def ignore_ploomber_stats_enabled_env_var(monkeypatch):
    """
    GitHub Actions configuration scripts set the PLOOMBER_STATS_ENABLED
    environment variable to prevent CI events from going to posthog, this
    inferes with some tests. This fixture removes its value temporarily
    """
    if 'PLOOMBER_STATS_ENABLED' in os.environ:
        monkeypatch.delenv('PLOOMBER_STATS_ENABLED', raising=True)


@pytest.fixture
def ignore_env_var_and_set_tmp_default_home_dir(
        tmp_directory, ignore_ploomber_stats_enabled_env_var, monkeypatch):
    """
    ignore_ploomber_stats_enabled_env_var + overrides DEFAULT_HOME_DIR
    to prevent the local configuration to interfere with tests
    """
    monkeypatch.setattr(telemetry, 'DEFAULT_HOME_DIR', '.')


# Test different modules are calling the telemetry module
@pytest.mark.parametrize(
    'custom_args',
    [[], ['--output', 'custom.png'], ['-o', 'custom.png'], ['--log', 'DEBUG'],
     ['-o', 'custom.png', '--log', 'DEBUG']])
def test_plot_uses_telemetry(custom_args, monkeypatch, tmp_directory):
    args_defaults = ['python', '--entry-point', 'test_pkg.entry.with_doc']
    monkeypatch.setattr(sys, 'argv', args_defaults + custom_args)
    mock = Mock()
    monkeypatch.setattr(dag_module.DAG, 'plot', mock)
    plot.main(catch_exception=False)

    assert mock.call_count == 1


setup_py = """
from setuptools import setup, find_packages

setup(
    name='sample_package',
    version='1.0',
    extras_require={'dev': []}
)
"""


@pytest.mark.parametrize('has_conda, use_lock, env, env_lock, reqs, reqs_lock',
                         [
                             [0, 0, 0, 0, 0, 0],
                             [0, 0, 0, 1, 0, 0],
                             [0, 0, 0, 0, 0, 1],
                             [0, 0, 0, 1, 0, 1],
                             [1, 0, 0, 0, 0, 0],
                             [1, 0, 0, 1, 0, 0],
                             [1, 0, 0, 0, 0, 1],
                             [1, 0, 0, 1, 0, 1],
                         ])
def test_install_lock_uses_telemetry(tmp_directory, has_conda, use_lock, env,
                                     env_lock, reqs, reqs_lock, monkeypatch):
    _prepare_files(has_conda, use_lock, env, env_lock, reqs, reqs_lock,
                   monkeypatch)
    Path('setup.py').write_text(setup_py)

    mock = Mock()
    monkeypatch.setattr(install.telemetry, "log_api", mock)

    with pytest.raises(SystemExit):
        install.main(use_lock=True if use_lock else False)

    assert mock.call_count == 2


def test_install_uses_telemetry(monkeypatch, tmp_directory):
    _write_sample_conda_env(env_name='telemetry')
    Path('setup.py').write_text(setup_py)

    mock = Mock()
    monkeypatch.setattr(install.telemetry, "log_api", mock)

    install.main(use_lock=False)
    assert mock.call_count == 2


@pytest.mark.parametrize('expected', [[
    call(action='build-started',
         package_name='ploomber',
         version=ver,
         metadata={
             'argv': ['python', '--entry-point', 'test_pkg.entry.with_doc']
         }),
    call(action='build-success',
         total_runtime='0:00:00',
         package_name='ploomber',
         version=ver,
         metadata={
             'argv': ['python', '--entry-point', 'test_pkg.entry.with_doc'],
             'dag': dag_module.DAG("No name")
         })
]])
def test_build_uses_telemetry(monkeypatch, tmp_directory, expected):
    _write_sample_conda_env()
    Path('setup.py').write_text(setup_py)

    mock = Mock()
    mock_dt = Mock()
    mock_dt.now.return_value = datetime.datetime(2022, 2, 24, 8, 16, 29)
    monkeypatch.setattr(sys, 'argv',
                        ['python', '--entry-point', 'test_pkg.entry.with_doc'])
    monkeypatch.setattr(build.telemetry, "log_api", mock)
    monkeypatch.setattr(telemetry.datetime, 'datetime', mock_dt)

    build.main(catch_exception=False)
    assert mock.call_args_list == expected


@pytest.mark.parametrize('args', [
    [],
    ['--source'],
    ['--source', '--build'],
    ['--force'],
    ['--force', '--build'],
    ['--status'],
    ['--status', '--build'],
    ['--on-finish'],
])
def test_task_command(args, tmp_nbs, monkeypatch):
    args = ['task', '--entry-point', 'pipeline.yaml', 'load'] + args
    monkeypatch.setattr(sys, 'argv', args)

    mock = Mock()
    monkeypatch.setattr(task.telemetry, "log_api", mock)
    task.main(catch_exception=False)

    assert mock.call_count == 2


def test_report_command(monkeypatch, tmp_directory):
    monkeypatch.setattr(sys, 'argv',
                        ['python', '--entry-point', 'test_pkg.entry.with_doc'])

    mock_log = Mock()
    mock_plot = Mock()
    monkeypatch.setattr(dag_module.DAG, 'plot', mock_plot)
    monkeypatch.setattr(report.telemetry, 'log_api', mock_log)

    report.main(catch_exception=False)

    assert mock_plot.call_count == 1


def test_status_command(monkeypatch):
    monkeypatch.setattr(sys, 'argv',
                        ['python', '--entry-point', 'test_pkg.entry.with_doc'])

    mock = Mock()
    monkeypatch.setattr(status.telemetry, "log_api", mock)
    status.main(catch_exception=False)

    assert mock.call_count == 2


def test_interact_uses_telemetry(monkeypatch, tmp_nbs):
    mock_start_ipython = Mock()
    monkeypatch.setattr(sys, 'argv', ['interact'])
    monkeypatch.setattr(interact, 'start_ipython', Mock())
    monkeypatch.setattr(interact.telemetry, 'log_api', mock_start_ipython)
    interact.main(catch_exception=False)

    assert mock_start_ipython.call_count == 2


def test_parse_dag_products(monkeypatch):
    product = '/ml-basic/output/get.parquet'
    dag = telemetry.clean_tasks_upstream_products(product)
    assert dag == 'get.parquet'

    products = {
        'nb': '/first-pipeline/output/get.ipynb',
        'data': '//first-pipeline/output/data.csv'
    }
    dag = telemetry.clean_tasks_upstream_products(products)
    assert dag == {'nb': 'get.ipynb', 'data': 'data.csv'}

    dag = telemetry.clean_tasks_upstream_products({})
    assert dag == {}


def test_parse_dag(monkeypatch, tmp_directory):
    def fn1(product):
        pass

    def fn2(upstream, product):
        pass

    dag = dag_module.DAG()
    t1 = PythonCallable(fn1,
                        File('/home/ido/filepath/file1.txt'),
                        dag,
                        name='first')
    t2 = PythonCallable(fn2,
                        File('/home/ido/filepath/file2.txt'),
                        dag,
                        name='second')
    t3 = PythonCallable(fn2,
                        File('/home/ido/filepath/file3.parquet'),
                        dag,
                        name='third')
    t1 >> t2
    t3 >> t2

    res = telemetry.parse_dag(dag)
    assert int(res['dag_size']) == 3
    assert res == {
        'dag_size': '3',
        'tasks': {
            'first': {
                'status': 'WaitingRender',
                'type': 'PythonCallable',
                'upstream': {},
                'products': 'file1.txt'
            },
            'third': {
                'status': 'WaitingRender',
                'type': 'PythonCallable',
                'upstream': {},
                'products': 'file3.parquet'
            },
            'second': {
                'status': 'WaitingRender',
                'type': 'PythonCallable',
                'upstream': {
                    'first': 'file1.txt',
                    'third': 'file3.parquet'
                },
                'products': 'file2.txt'
            }
        }
    }


def test_creates_config_directory(monkeypatch, tmp_nbs,
                                  ignore_ploomber_stats_enabled_env_var):
    monkeypatch.setattr(telemetry, 'DEFAULT_HOME_DIR', '.')
    monkeypatch.setattr(sys, 'argv', ['status'])

    status.main(catch_exception=False)

    assert Path('stats').is_dir()
    assert Path('stats', 'uid.yaml').is_file()
    assert Path('stats', 'config.yaml').is_file()


expected_dag_dict = {
    'dag_size': '3',
    'tasks': {
        'load': {
            'status': 'WaitingRender',
            'type': 'NotebookRunner',
            'upstream': {},
            'products': {
                'nb': 'load.ipynb',
                'data': 'data.csv'
            }
        },
        'clean': {
            'status': 'WaitingRender',
            'type': 'NotebookRunner',
            'upstream': {
                'load': "data.csv')}"
            },
            'products': {
                'nb': 'clean.ipynb',
                'data': 'clean.csv'
            }
        },
        'plot': {
            'status': 'WaitingRender',
            'type': 'NotebookRunner',
            'upstream': {
                'clean': "clean.csv')}"
            },
            'products': 'plot.ipynb'
        }
    }
}


@pytest.fixture
def mock_posthog_capture(monkeypatch):
    mock = Mock()
    mock_dt = Mock()
    mock_dt.now.side_effect = [1, 2, 3]
    monkeypatch.setattr(telemetry.posthog, 'capture', mock)
    monkeypatch.setattr(telemetry, '_get_telemetry_info', lambda:
                        (True, 'UID', False))
    return mock


@pytest.mark.xfail(sys.platform == "win32", reason="bug in parse_dag")
def test_parses_dag(mock_posthog_capture, tmp_nbs):
    @telemetry.log_call('some-action', 'ploomber', ver, payload=True)
    def my_function(payload):
        payload['dag'] = DAGSpec('pipeline.yaml').to_dag()

    my_function()

    call2_kwargs = mock_posthog_capture.call_args_list[1][1]
    assert call2_kwargs['properties']['metadata']['dag'] == expected_dag_dict


@pytest.mark.xfail(sys.platform == "win32", reason="bug in parse_dag")
def test_parses_dag_on_exception(mock_posthog_capture, tmp_nbs):
    @telemetry.log_call('some-action', 'ploomber', ver, payload=True)
    def my_function(payload):
        payload['dag'] = DAGSpec('pipeline.yaml').to_dag()
        raise BaseException('some error', type_='some-type')

    with pytest.raises(BaseException):
        my_function()

    call2_kwargs = mock_posthog_capture.call_args_list[1][1]
    assert call2_kwargs['properties']['metadata']['dag'] == expected_dag_dict
