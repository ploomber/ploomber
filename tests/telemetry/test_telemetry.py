import os

import click
import sys
from unittest.mock import Mock
from pathlib import Path
import pytest
from ploomber.telemetry import telemetry
from ploomber.telemetry.validate_inputs import str_param, opt_str_param
from ploomber.cli import plot, install, build, interact, task, report, status
import ploomber.dag.dag as dag_module
from conftest import _write_sample_conda_env, _prepare_files


# Validations tests
def test_str_validation():
    res = str_param("Test", "")
    assert isinstance(res, str)
    res = str_param("TEST", "test_param")
    assert 'TEST' == res
    with pytest.raises(TypeError) as exc_info:
        str_param(3, "Test_number")

    exception_raised = exc_info.value
    assert type(exception_raised) == TypeError


def test_opt_str_validation():
    res = opt_str_param("Test", "")
    assert isinstance(res, str)
    res = opt_str_param("Test", "TEST")
    assert 'TEST' == res
    res = opt_str_param("Test", None)
    assert not res

    with pytest.raises(TypeError) as exc_info:
        opt_str_param("Test", 3)

    exception_raised = exc_info.value
    assert type(exception_raised) == TypeError


# Test class functions
def test_stats_file():
    stats_enabled = telemetry.check_stats_file()
    assert isinstance(stats_enabled, bool)
    assert stats_enabled


def test_uid_file():
    uid = telemetry.check_uid()
    assert isinstance(uid, str)


def test_full_telemetry_info():
    (stats_enabled, uid) = telemetry._get_telemetry_info()
    assert isinstance(stats_enabled, bool)
    assert stats_enabled
    assert isinstance(uid, str)


def test_basedir_creation():
    base_dir = telemetry.check_dir_file_exist()
    assert os.path.exists(base_dir)


def test_python_version():
    version = telemetry.python_version()
    assert isinstance(version, str)


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
def test_install_lock_exception(tmp_directory, has_conda, use_lock, env,
                                env_lock, reqs, reqs_lock, monkeypatch):

    _prepare_files(has_conda, use_lock, env, env_lock, reqs, reqs_lock,
                   monkeypatch)
    Path('setup.py').write_text(setup_py)

    mock = Mock()
    monkeypatch.setattr(install.telemetry, "log_api", mock)

    with pytest.raises(click.ClickException):
        install.main(True if use_lock else False)

    assert mock.call_count == 2


def test_install_uses_telemetry(monkeypatch, tmp_directory):
    _write_sample_conda_env(env_name='telemetry')
    Path('setup.py').write_text(setup_py)

    mock = Mock()
    monkeypatch.setattr(install.telemetry, "log_api", mock)

    install.main(False)
    assert mock.call_count == 2


def test_build_uses_telemetry(monkeypatch, tmp_directory):
    _write_sample_conda_env()
    Path('setup.py').write_text(setup_py)

    mock = Mock()
    monkeypatch.setattr(sys, 'argv',
                        ['python', '--entry-point', 'test_pkg.entry.with_doc'])
    monkeypatch.setattr(build.telemetry, "log_api", mock)

    build.main(catch_exception=False)
    assert mock.call_count == 1


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

    assert mock.call_count == 1


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

    assert mock.call_count == 1


def test_interact_uses_telemetry(monkeypatch, tmp_nbs):
    mock_start_ipython = Mock()
    monkeypatch.setattr(sys, 'argv', ['interact'])
    monkeypatch.setattr(interact, 'start_ipython', mock_start_ipython)
    monkeypatch.setattr(interact.telemetry, 'log_api', mock_start_ipython)
    interact.main(catch_exception=False)

    assert mock_start_ipython.call_count == 2


def test_stats_off(monkeypatch):
    mock = Mock()
    posthog_mock = Mock()
    mock.patch(telemetry, '_get_telemetry_info', (False, 'TestUID'))
    telemetry.log_api("test_action")

    assert posthog_mock.call_count == 0


def test_offline_stats(monkeypatch):
    mock = Mock()
    posthog_mock = Mock()
    mock.patch(telemetry, 'is_online', False)
    telemetry.log_api("test_action")

    assert posthog_mock.call_count == 0


def test_online_input(monkeypatch):
    with pytest.raises(ValueError) as exc_info:
        telemetry.is_online("test_action")

    exception_raised = exc_info.value
    assert type(exception_raised) == ValueError


def test_online_func(monkeypatch):
    online = telemetry.is_online()
    assert online


def test_validate_entries(monkeypatch):
    event_id = 'event_id'
    uid = 'uid'
    action = 'action'
    client_time = 'client_time'
    elapsed_time = 'elapsed_time'
    res = telemetry.validate_entries(event_id, uid, action, client_time,
                                     elapsed_time)
    assert res == (event_id, uid, action, client_time, elapsed_time)


def test_python_major_version():
    version = telemetry.python_version()
    major = version.split(".")[0]
    assert int(major) == 3


def test_creates_config_directory(monkeypatch, tmp_nbs):
    monkeypatch.setattr(telemetry, 'DEFAULT_HOME_DIR', '.')
    monkeypatch.setattr(sys, 'argv', ['status'])

    status.main(catch_exception=False)

    assert Path('stats').is_dir()
    assert Path('stats', 'uid.yaml').is_file()
    assert Path('stats', 'config.yaml').is_file()
