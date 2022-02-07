import datetime
import pathlib
import click
import sys
from unittest.mock import Mock
from pathlib import Path
import pytest

import yaml

import ploomber
from ploomber.telemetry import telemetry
from ploomber.telemetry.validate_inputs import str_param, opt_str_param
from ploomber.cli import plot, install, build, interact, task, report, status
import ploomber.dag.dag as dag_module
from ploomber.tasks.tasks import PythonCallable
from ploomber.products.file import File
from conftest import _write_sample_conda_env, _prepare_files


@pytest.fixture()
def inside_conda_env(monkeypatch):
    monkeypatch.setenv('CONDA_PREFIX', True)


@pytest.fixture()
def inside_pip_env(monkeypatch):
    monkeypatch.setattr(telemetry.sys, 'prefix', 'sys.prefix')
    monkeypatch.setattr(telemetry.sys, 'base_prefix', 'base_prefix')


@pytest.fixture
def ignore_ploomber_stats_enabled_env_var(monkeypatch):
    """
    GitHub Actions configuration scripts set the PLOOMBER_STATS_ENABLED
    environment variable to prevent CI events from going to posthog, this
    inferes with some tests. This fixture removes its value temporarily
    """
    monkeypatch.delenv('PLOOMBER_STATS_ENABLED', raising=True)


@pytest.fixture
def ignore_env_var_and_set_tmp_default_home_dir(
        tmp_directory, ignore_ploomber_stats_enabled_env_var, monkeypatch):
    """
    ignore_ploomber_stats_enabled_env_var + overrides DEFAULT_HOME_DIR
    to prevent the local configuration to interfere with tests
    """
    monkeypatch.setattr(telemetry, 'DEFAULT_HOME_DIR', '.')


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


def test_check_stats_enabled(ignore_env_var_and_set_tmp_default_home_dir):
    stats_enabled = telemetry.check_stats_enabled()
    assert stats_enabled is True


@pytest.mark.parametrize(
    'yaml_value, expected_first, env_value, expected_second', [
        ['true', True, 'false', False],
        ['TRUE', True, 'FALSE', False],
        ['false', False, 'true', True],
        ['FALSE', False, 'TRUE', True],
    ])
def test_env_var_takes_precedence(monkeypatch,
                                  ignore_env_var_and_set_tmp_default_home_dir,
                                  yaml_value, expected_first, env_value,
                                  expected_second):

    stats = Path('stats')
    stats.mkdir()

    (stats / 'config.yaml').write_text(f"""
                                        stats_enabled: {yaml_value}
                                        """)

    assert telemetry.check_stats_enabled() is expected_first

    monkeypatch.setenv('PLOOMBER_STATS_ENABLED', env_value, prepend=False)

    assert telemetry.check_stats_enabled() is expected_second


def test_first_usage(monkeypatch, tmp_directory):
    monkeypatch.setattr(telemetry, 'DEFAULT_HOME_DIR', '.')

    stats = Path('stats')
    stats.mkdir()

    # This isn't a first time usage since the config file doesn't exist yet.
    assert not telemetry.check_first_time_usage()
    (stats / 'config.yaml').write_text("stats_enabled: True")

    assert telemetry.check_first_time_usage()


# The below fixtures are to mock the different virtual environments
# Ref: https://stackoverflow.com/questions/51266880/detect-if-
# python-is-running-in-a-conda-environment
def test_conda_env(monkeypatch, inside_conda_env, tmp_directory):
    # Set a conda parameterized env
    env = telemetry.is_conda()
    assert env is True
    env = telemetry.get_env()
    assert env == 'conda'


# Ref: https://stackoverflow.com/questions/1871549/
# determine-if-python-is-running-inside-virtualenv
def test_pip_env(monkeypatch, inside_pip_env):
    # Set a pip parameterized env
    env = telemetry.in_virtualenv()
    assert env is True
    env = telemetry.get_env()
    assert env == 'pip'


# Ref: https://stackoverflow.com/questions/43878953/how-does-one-detect-if-
# one-is-running-within-a-docker-container-within-python
def test_docker_env(monkeypatch):
    def mock(input_path):
        return 'dockerenv' in str(input_path)

    monkeypatch.setattr(pathlib.Path, 'exists', mock)
    docker = telemetry.is_docker()
    assert docker is True


# Ref https://stackoverflow.com/questions/53581278/test-if-
# notebook-is-running-on-google-colab
def test_colab_env(monkeypatch):
    monkeypatch.setenv('COLAB_GPU', True)
    colab = telemetry.is_colab()
    assert colab is True


# Ref https://learn.paperspace.com/video/creating-a-jupyter-notebook
@pytest.mark.parametrize(
    'env_variable',
    ['PS_API_KEY', 'PAPERSPACE_API_KEY', 'PAPERSPACE_NOTEBOOK_REPO_ID'])
def test_paperspace_env(monkeypatch, env_variable):
    monkeypatch.setenv(env_variable, True)
    pspace = telemetry.is_paperspace()
    assert pspace is True


# Ref https://stackoverflow.com/questions/63298054/how-to-check-if-my-code
# -runs-inside-a-slurm-environment
def test_slurm_env(monkeypatch):
    monkeypatch.setenv('SLURM_JOB_ID', True)
    slurm = telemetry.is_slurm()
    assert slurm is True


# Ref https://airflow.apache.org/docs/apache-airflow/stable/
# cli-and-env-variables-ref.html?highlight=airflow_home#envvar-AIRFLOW_HOME
@pytest.mark.parametrize('env_variable', ['AIRFLOW_CONFIG', 'AIRFLOW_HOME'])
def test_airflow_env(monkeypatch, env_variable):
    monkeypatch.setenv(env_variable, True)
    airflow = telemetry.is_airflow()
    assert airflow is True


# Ref https://stackoverflow.com/questions/110362/how-can-i-find-
# the-current-os-in-python
@pytest.mark.parametrize('os_param', ['Windows', 'Linux', 'MacOS', 'Ubuntu'])
def test_os_type(monkeypatch, os_param):
    mock = Mock()
    mock.return_value = os_param
    monkeypatch.setattr(telemetry.platform, 'system', mock)
    os_type = telemetry.get_os()
    assert os_type == os_param


def test_uid_file():
    uid = telemetry.check_uid()
    assert isinstance(uid, str)


def test_full_telemetry_info(ignore_env_var_and_set_tmp_default_home_dir):
    (stats_enabled, uid, is_install) = telemetry._get_telemetry_info()
    assert stats_enabled is True
    assert isinstance(uid, str)
    assert is_install is True


def test_basedir_creation():
    base_dir = telemetry.check_dir_exist()
    assert base_dir.exists()


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


def test_is_online():
    assert telemetry.is_online()


def test_parse_dag_products(monkeypatch):
    product = '/ml-basic/output/get.parquet'
    dag = telemetry.clean_tasks_upstream_products(product)
    assert dag == 'get.parquet'

    products = {
        'nb': '/spec-api-python/output/get.ipynb',
        'data': '//spec-api-python/output/data.csv'
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
    assert res['dag_size'] == 3
    assert res == {
        'dag_size': 3,
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


def test_is_not_online(monkeypatch):
    mock_httplib = Mock()
    mock_httplib.HTTPSConnection().request.side_effect = Exception
    monkeypatch.setattr(telemetry, 'httplib', mock_httplib)

    assert not telemetry.is_online()


def test_validate_entries(monkeypatch):
    event_id = 'event_id'
    uid = 'uid'
    action = 'action'
    client_time = 'client_time'
    elapsed_time = 'elapsed_time'
    res = telemetry.validate_entries(event_id, uid, action, client_time,
                                     elapsed_time)
    assert res == (event_id, uid, action, client_time, elapsed_time)


def test_get_latest_version(monkeypatch):
    is_latest = telemetry.get_latest_version()
    assert isinstance(is_latest, str)
    version_index = [i for i, ltr in enumerate(is_latest) if ltr == '.']
    assert len(version_index) >= 1

    mock_httplib = Mock()
    mock_httplib.HTTPSConnection().request.side_effect = Exception
    monkeypatch.setattr(telemetry, 'httplib', mock_httplib)
    is_latest = telemetry.get_latest_version()
    assert is_latest == ploomber.__version__

    # Mock version and the conf, check it produces the same version


def write_to_conf_file(tmp_directory, monkeypatch, last_check):
    stats = Path('stats')
    stats.mkdir()
    path = stats / 'config.yaml'
    monkeypatch.setattr(telemetry, 'DEFAULT_HOME_DIR', '.')
    (path).write_text("stats_enabled: True\n"
                      f"version_check_enabled: {last_check}\n")


def test_version_skips_when_updated(tmp_directory, capsys, monkeypatch):
    # Path conf file
    monkeypatch.setattr(ploomber, '__version__', '0.14.8')
    mock_version = Mock()
    mock_version.return_value = '0.14.8'
    monkeypatch.setattr(telemetry, 'get_latest_version', mock_version)

    write_to_conf_file(
        tmp_directory=tmp_directory,
        monkeypatch=monkeypatch,
        last_check='2022-01-20 10:51:41.082376')  # version='0.14.8',

    # Test no warning when same version encountered
    # telemetry.check_version()
    # captured = capsys.readouterr()
    # assert "ploomber version" not in captured.out


def test_user_output_on_different_versions(tmp_directory, capsys, monkeypatch):
    mock_version = Mock()
    monkeypatch.setattr(telemetry, 'get_latest_version', mock_version)
    write_to_conf_file(tmp_directory=tmp_directory,
                       monkeypatch=monkeypatch,
                       last_check='2022-01-20 10:51:41.082376')
    mock_version.return_value = '0.14.0'

    # Check now that the date is different there is an upgrade warning
    telemetry.check_version()
    captured = capsys.readouterr()
    assert "ploomber version" in captured.out


def test_no_output_latest_version(tmp_directory, capsys, monkeypatch):
    # The file's date is today now, no error should be raised
    write_to_conf_file(tmp_directory=tmp_directory,
                       monkeypatch=monkeypatch,
                       last_check=datetime.datetime.now())
    telemetry.check_version()
    captured = capsys.readouterr()
    assert "ploomber version" not in captured.out


def test_output_on_date_diff(tmp_directory, capsys, monkeypatch):
    # Warning should be caught since the date and version are off
    write_to_conf_file(tmp_directory=tmp_directory,
                       monkeypatch=monkeypatch,
                       last_check='2022-01-20 10:51:41.082376')
    path = Path('stats') / 'config.yaml'
    telemetry.check_version()
    captured = capsys.readouterr()
    assert "ploomber version" in captured.out

    # Check the conf file was updated
    with path.open("r") as file:
        conf = yaml.safe_load(file)
    diff = (datetime.datetime.now() - conf['version_check_enabled']).days
    assert diff == 0


def test_python_major_version():
    version = telemetry.python_version()
    major = version.split(".")[0]
    assert int(major) == 3


def test_creates_config_directory(monkeypatch, tmp_nbs,
                                  ignore_ploomber_stats_enabled_env_var):
    monkeypatch.setattr(telemetry, 'DEFAULT_HOME_DIR', '.')
    monkeypatch.setattr(sys, 'argv', ['status'])

    status.main(catch_exception=False)

    assert Path('stats').is_dir()
    assert Path('stats', 'uid.yaml').is_file()
    assert Path('stats', 'config.yaml').is_file()
