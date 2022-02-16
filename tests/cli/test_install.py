import subprocess
import os
import sys
from pathlib import Path
from unittest.mock import Mock, call
import shutil
import datetime
import pytest
from click.testing import CliRunner
from ploomber.cli import install as install_module
from ploomber.cli.cli import install
from ploomber.cli.install import _pip_install
from conftest import _write_sample_conda_env, _prepare_files,\
     _write_sample_pip_req

setup_py = """
from setuptools import setup, find_packages

setup(
    name='sample_package',
    version='1.0',
    extras_require={'dev': []}
)
"""


@pytest.fixture
def pkg_manager():
    mamba = shutil.which('mamba')
    conda = shutil.which('conda')
    return mamba if mamba else conda


@pytest.fixture
def error_if_calling_locate_pip_inside_conda(monkeypatch):
    # to make it fail if it attempts to look for pip, see docstring in the
    # '_locate_pip_inside_conda' method for details
    mock_locate = Mock(side_effect=ValueError)
    monkeypatch.setattr(install_module, '_locate_pip_inside_conda',
                        mock_locate)


@pytest.fixture
def mock_cmdr_wrapped(monkeypatch):
    mock = Mock(wraps=install_module.Commander().run)
    monkeypatch.setattr(install_module.Commander, 'run', mock)
    return mock


@pytest.fixture
def cleanup_conda_tmp_env():
    if os.name == 'nt':
        conda = shutil.which('conda')
        subprocess.run([conda, 'env', 'remove', '--name', 'my_tmp_env'])


def _get_venv_and_pip():
    name = Path().resolve().name
    venv = f'venv-{name}'
    pip = str(
        Path(venv, 'Scripts' if os.name == 'nt' else 'bin',
             'pip.exe' if os.name == 'nt' else 'pip'))
    return venv, pip


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
def test_missing_both_files(tmp_directory, has_conda, use_lock, env, env_lock,
                            reqs, reqs_lock, monkeypatch):

    _prepare_files(has_conda, use_lock, env, env_lock, reqs, reqs_lock,
                   monkeypatch)

    runner = CliRunner()
    result = runner.invoke(install,
                           args=['--use-lock'] if use_lock else [],
                           catch_exceptions=False)
    expected = ('Expected an environment.yaml (conda)'
                ' or requirements.txt (pip) in the current directory.'
                ' Add one of them and try again.')

    assert f'Error: {expected}\n' == result.stdout


@pytest.mark.parametrize('has_conda, use_lock, env, env_lock, reqs, reqs_lock',
                         [
                             [0, 1, 1, 0, 1, 0],
                             [1, 1, 1, 0, 1, 0],
                             [0, 1, 0, 0, 1, 0],
                             [1, 1, 0, 0, 1, 0],
                             [0, 1, 1, 0, 0, 0],
                             [1, 1, 1, 0, 0, 0],
                             [0, 1, 0, 0, 0, 0],
                             [1, 1, 0, 0, 0, 0],
                         ])
def test_missing_both_lock_files(tmp_directory, has_conda, use_lock, env,
                                 env_lock, reqs, reqs_lock, monkeypatch):

    _prepare_files(has_conda, use_lock, env, env_lock, reqs, reqs_lock,
                   monkeypatch)

    runner = CliRunner()
    result = runner.invoke(install,
                           args=['--use-lock'] if use_lock else [],
                           catch_exceptions=False)
    expected = (
        'Expected and environment.lock.yaml '
        '(conda) or requirements.lock.txt (pip) in the current directory. '
        'Add one of them and try again.')

    assert f'Error: {expected}\n' == result.stdout


@pytest.mark.parametrize('has_conda, use_lock, env, env_lock, reqs, reqs_lock',
                         [
                             [0, 1, 1, 1, 1, 0],
                             [0, 1, 0, 1, 1, 0],
                             [0, 1, 1, 1, 0, 0],
                             [0, 1, 0, 1, 0, 0],
                         ])
def test_missing_env_lock(tmp_directory, has_conda, use_lock, env, env_lock,
                          reqs, reqs_lock, monkeypatch):

    _prepare_files(has_conda, use_lock, env, env_lock, reqs, reqs_lock,
                   monkeypatch)

    runner = CliRunner()
    result = runner.invoke(install,
                           args=['--use-lock'] if use_lock else [],
                           catch_exceptions=False)
    expected = ('Found env environment.lock.yaml '
                'but conda is not installed. Install conda or add a '
                'requirements.lock.txt to use pip instead')

    assert f'Error: {expected}\n' == result.stdout


@pytest.mark.parametrize('has_conda, use_lock, env, env_lock, reqs, '
                         'reqs_lock', [
                             [0, 0, 1, 0, 0, 1],
                             [0, 0, 1, 1, 0, 1],
                             [0, 0, 1, 0, 0, 0],
                             [0, 0, 1, 1, 0, 0],
                         ])
def test_missing_env(tmp_directory, has_conda, use_lock, env, env_lock, reqs,
                     reqs_lock, monkeypatch):

    _prepare_files(has_conda, use_lock, env, env_lock, reqs, reqs_lock,
                   monkeypatch)

    runner = CliRunner()
    result = runner.invoke(install,
                           args=['--use-lock'] if use_lock else [],
                           catch_exceptions=False)
    expected = ('Found environment.yaml but conda '
                'is not installed. Install conda or add a '
                'requirements.txt to use pip instead')

    assert f'Error: {expected}\n' == result.stdout


def mocked_get_now():
    dt = datetime.datetime(2021, 1, 1, 10, 10, 10)
    return dt


@pytest.mark.parametrize('has_conda, use_lock, env, env_lock, reqs, reqs_lock',
                         [
                             [1, 1, 1, 1, 1, 0],
                             [1, 1, 1, 1, 1, 1],
                             [1, 1, 1, 1, 0, 0],
                             [1, 1, 1, 1, 0, 1],
                             [1, 1, 0, 1, 1, 0],
                             [1, 1, 0, 1, 1, 1],
                             [1, 1, 0, 1, 0, 0],
                             [1, 1, 0, 1, 0, 1],
                             [1, 0, 1, 0, 1, 0],
                             [1, 0, 1, 1, 1, 0],
                             [1, 0, 1, 0, 1, 1],
                             [1, 0, 1, 1, 1, 1],
                             [1, 0, 1, 0, 0, 0],
                             [1, 0, 1, 1, 0, 0],
                             [1, 0, 1, 0, 0, 1],
                             [1, 0, 1, 1, 0, 1],
                         ])
def test_install_with_conda(tmp_directory, has_conda, use_lock, env, env_lock,
                            reqs, reqs_lock, monkeypatch):

    _prepare_files(has_conda, use_lock, env, env_lock, reqs, reqs_lock,
                   monkeypatch)

    mock = Mock()
    mock.patch(install_module, 'datetime', side_effect=mocked_get_now)
    monkeypatch.setattr(install_module, 'main_conda', mock)
    start = mocked_get_now()
    install_module.main_conda(start, True if use_lock else False)
    inputs_args, kwargs = mock.call_args

    assert inputs_args[0] == start
    assert inputs_args[1] == bool(use_lock)


@pytest.mark.parametrize('has_conda, use_lock, env, env_lock, reqs, reqs_lock',
                         [
                             [1, 1, 1, 0, 1, 1],
                             [1, 1, 0, 0, 1, 1],
                             [1, 1, 1, 0, 0, 1],
                             [1, 1, 0, 0, 0, 1],
                             [0, 1, 1, 0, 1, 1],
                             [0, 1, 1, 1, 1, 1],
                             [0, 1, 0, 0, 1, 1],
                             [0, 1, 0, 1, 1, 1],
                             [0, 1, 1, 0, 0, 1],
                             [0, 1, 1, 1, 0, 1],
                             [0, 1, 0, 0, 0, 1],
                             [0, 1, 0, 1, 0, 1],
                             [1, 0, 0, 0, 1, 0],
                             [1, 0, 0, 1, 1, 0],
                             [1, 0, 0, 0, 1, 1],
                             [1, 0, 0, 1, 1, 1],
                             [0, 0, 1, 0, 1, 0],
                             [0, 0, 1, 1, 1, 0],
                             [0, 0, 1, 0, 1, 1],
                             [0, 0, 1, 1, 1, 1],
                             [0, 0, 0, 0, 1, 0],
                             [0, 0, 0, 1, 1, 0],
                             [0, 0, 0, 0, 1, 1],
                             [0, 0, 0, 1, 1, 1],
                         ])
def test_install_with_pip(tmp_directory, has_conda, use_lock, env, env_lock,
                          reqs, reqs_lock, monkeypatch):

    _prepare_files(has_conda, use_lock, env, env_lock, reqs, reqs_lock,
                   monkeypatch)

    mock = Mock()
    monkeypatch.setattr(install_module, 'main_pip', mock)
    mock.patch(install_module, 'datetime', side_effect=mocked_get_now)
    start = mocked_get_now()
    install_module.main_pip(start, True if use_lock else False)
    inputs_args, kwargs = mock.call_args

    assert inputs_args[0] == start
    assert inputs_args[1] == bool(use_lock)


@pytest.mark.parametrize('conda_bin, conda_root',
                         [
                             [('something', 'Miniconda3', 'conda'),
                              ('something', 'Miniconda3')],
                             [('something', 'miniconda3', 'conda'),
                              ('something', 'miniconda3')],
                             [('something', 'Anaconda3', 'conda'),
                              ('something', 'Anaconda3')],
                             [('something', 'anaconda3', 'conda'),
                              ('something', 'anaconda3')],
                             [('one', 'miniconda3', 'dir', 'conda'),
                              ('one', 'miniconda3')],
                             [('one', 'anaconda3', 'dir', 'conda'),
                              ('one', 'anaconda3')],
                             [('one', 'another', 'Miniconda3', 'conda'),
                              ('one', 'another', 'Miniconda3')],
                             [('one', 'another', 'Anaconda3', 'conda'),
                              ('one', 'another', 'Anaconda3')],
                         ])
def test_find_conda_root(conda_bin, conda_root):
    assert install_module._find_conda_root(
        Path(*conda_bin)).parts == conda_root


def test_error_if_unknown_conda_layout():
    with pytest.raises(RuntimeError):
        install_module._find_conda_root(Path('a', 'b'))


@pytest.mark.parametrize(
    'conda_bin',
    [
        # old versions of conda may have the conda binary in a different
        # location see #319
        ['Users', 'user', 'Miniconda3', 'Library', 'bin', 'conda.BAT'],
        ['Users', 'user', 'Miniconda3', 'condabin', 'conda.BAT'],
    ],
    ids=['location-old', 'location-new'])
def test_locate_pip_inside_conda(monkeypatch, tmp_directory, conda_bin):
    mock = Mock(return_value=str(Path(*conda_bin)))

    path = Path('Users', 'user', 'Miniconda3', 'envs', 'myenv',
                'Scripts' if os.name == 'nt' else 'bin',
                'pip.exe' if os.name == 'nt' else 'pip')

    path.parent.mkdir(parents=True)
    path.touch()

    monkeypatch.setattr(install_module.shutil, 'which', mock)

    assert install_module._locate_pip_inside_conda('myenv') == str(path)


# FIXME: i tested this locally on a windows machine and it works but for some
# reason, the machine running on github actions is unable to locate "conda"
# hence this fails. it's weird because I'm calling conda without issues
# to install dependencies during setup. Same with the next two tests
@pytest.mark.xfail(sys.platform == 'win32',
                   reason='Test not working on Github Actions on Windows')
def test_install_package_conda(tmp_directory, mock_cmdr_wrapped):
    _write_sample_conda_env()
    Path('setup.py').write_text(setup_py)

    runner = CliRunner()
    runner.invoke(install, catch_exceptions=False)

    # check it calls "pip install --editable ."
    assert mock_cmdr_wrapped.call_args_list[-2][1][
        'description'] == 'Installing project'

    # check first argument is the path to the conda binary instead of just
    # "conda" since we discovered that fails sometimes on Windows
    assert all(
        [Path(c[0][0]).is_file() for c in mock_cmdr_wrapped.call_args_list])

    assert set(os.listdir()) == {
        'environment.yml',
        'environment.lock.yml',
        'sample_package.egg-info',
        'setup.py',
    }


@pytest.mark.xfail(sys.platform == 'win32',
                   reason='Test not working on Github Actions on Windows')
def test_install_non_package_with_conda(tmp_directory, monkeypatch,
                                        mock_cmdr_wrapped):
    # to make it fail if it attempts to look for pip, see docstring in the
    # '_locate_pip_inside_conda' method for details
    mock_locate = Mock(side_effect=ValueError)
    monkeypatch.setattr(install_module, '_locate_pip_inside_conda',
                        mock_locate)

    _write_sample_conda_env()

    runner = CliRunner()
    runner.invoke(install, catch_exceptions=False)

    # check first argument is the path to the conda binary instead of just
    # "conda" since we discovered that fails sometimes on Windows
    assert all(
        [Path(c[0][0]).is_file() for c in mock_cmdr_wrapped.call_args_list])

    assert set(os.listdir()) == {'environment.yml', 'environment.lock.yml'}


@pytest.mark.xfail(sys.platform == 'win32',
                   reason='Test not working on Github Actions on Windows')
def test_non_package_with_conda_with_dev_deps(tmp_directory):
    _write_sample_conda_env()
    _write_sample_conda_env('environment.dev.yml')
    runner = CliRunner()

    runner.invoke(install, catch_exceptions=False)

    assert set(os.listdir()) == {
        'environment.yml', 'environment.lock.yml', 'environment.dev.yml',
        'environment.dev.lock.yml'
    }


@pytest.mark.parametrize('create_dev_lock', [True, False])
def test_install_lock_non_package_with_conda(
        tmp_directory, monkeypatch, mock_cmdr_wrapped, pkg_manager,
        error_if_calling_locate_pip_inside_conda, cleanup_conda_tmp_env,
        create_dev_lock):
    _write_sample_conda_env('environment.lock.yml')

    if create_dev_lock:
        _write_sample_conda_env('environment.dev.lock.yml')

    runner = CliRunner()
    runner.invoke(install, args='--use-lock', catch_exceptions=False)

    expected = [
        call(pkg_manager,
             'env',
             'create',
             '--file',
             'environment.lock.yml',
             '--force',
             description='Creating env'),
        call(pkg_manager,
             'env',
             'update',
             '--file',
             'environment.dev.lock.yml',
             description='Installing dev dependencies')
    ]

    if os.name == 'nt':
        expected.insert(
            0, call(pkg_manager, 'env', 'list', '--json', capture_output=True))

    if not create_dev_lock:
        expected.pop(-1)

    assert mock_cmdr_wrapped.call_args_list == expected
    assert all(
        [Path(c[0][0]).is_file() for c in mock_cmdr_wrapped.call_args_list])


@pytest.mark.parametrize('create_dev_lock', [True, False])
def test_install_lock_package_with_conda(tmp_directory, monkeypatch,
                                         mock_cmdr_wrapped, pkg_manager,
                                         cleanup_conda_tmp_env,
                                         create_dev_lock):
    _write_sample_conda_env('environment.lock.yml')

    if create_dev_lock:
        _write_sample_conda_env('environment.dev.lock.yml')

    Path('setup.py').write_text(setup_py)

    runner = CliRunner()
    runner.invoke(install, args='--use-lock', catch_exceptions=False)

    pip = install_module._path_to_pip_in_env_with_name(shutil.which('conda'),
                                                       'my_tmp_env')

    expected = [
        call(pkg_manager,
             'env',
             'create',
             '--file',
             'environment.lock.yml',
             '--force',
             description='Creating env'),
        call(pip,
             'install',
             '--editable',
             '.',
             description='Installing project'),
        call(pkg_manager,
             'env',
             'update',
             '--file',
             'environment.dev.lock.yml',
             description='Installing dev dependencies')
    ]

    if os.name == 'nt':
        expected.insert(
            0, call(pkg_manager, 'env', 'list', '--json', capture_output=True))

    if not create_dev_lock:
        expected.pop(-1)

    assert mock_cmdr_wrapped.call_args_list == expected
    assert all(
        [Path(c[0][0]).is_file() for c in mock_cmdr_wrapped.call_args_list])


# FIXME: I tested this locally on a windows machine but breaks on Github
# Actions.
# Problem happens when running pip:
# AssertionError: Egg-link c:\users\runner~1\appdata\local\temp\tmp30cvb5ki
# does not match installed location of sample-package-pip
# (at c:\users\runneradmin\appdata\local\temp\tmp30cvb5ki)
# I think it's because of some weird configuration on github actions
# creates symlinks
@pytest.mark.xfail(sys.platform == 'win32',
                   reason='Test not working on Github Actions on Windows')
def test_install_pip(tmp_directory):
    _write_sample_pip_req()

    Path('setup.py').write_text(setup_py)
    name = f'venv-{Path(tmp_directory).name}'

    runner = CliRunner()
    result = runner.invoke(install, catch_exceptions=False)

    if os.name == 'nt':
        expected_command = (
            f'\nIf using cmd.exe: {name}\\Scripts\\activate.bat'
            f'\nIf using PowerShell: {name}\\Scripts\\Activate.ps1')
    else:
        expected_command = f'source {name}/bin/activate'

    assert Path('.gitignore').read_text() == f'\n{name}\n'
    assert expected_command in result.stdout
    assert Path('requirements.lock.txt').exists()
    assert result.exit_code == 0


@pytest.mark.xfail(sys.platform == 'win32',
                   reason='Test not working on Github Actions on Windows')
def test_install_pip_does_not_duplicate_gitignore_entry(tmp_directory):
    _write_sample_pip_req()

    Path('setup.py').write_text(setup_py)
    name = f'venv-{Path(tmp_directory).name}'

    Path('.gitignore').write_text(f'{name}\n')

    runner = CliRunner()
    result = runner.invoke(install, catch_exceptions=False)

    # the entry was already there, should not duplicate
    assert Path('.gitignore').read_text() == f'{name}\n'
    assert result.exit_code == 0


def test_non_package_with_pip(tmp_directory):
    _write_sample_pip_req()

    Path('setup.py').write_text(setup_py)
    name = f'venv-{Path(tmp_directory).name}'

    runner = CliRunner()
    result = runner.invoke(install, catch_exceptions=False)

    assert Path('.gitignore').read_text() == f'\n{name}\n'
    assert Path('requirements.lock.txt').exists()
    assert result.exit_code == 0


def test_non_package_with_pip_with_dev_deps(tmp_directory):
    _write_sample_pip_req()
    _write_sample_pip_req('requirements.dev.txt')

    Path('setup.py').write_text(setup_py)
    name = f'venv-{Path(tmp_directory).name}'

    runner = CliRunner()
    result = runner.invoke(install, catch_exceptions=False)

    assert Path('.gitignore').read_text() == f'\n{name}\n'
    assert Path('requirements.lock.txt').exists()
    assert Path('requirements.dev.lock.txt').exists()
    assert '# Editable install' not in Path(
        'requirements.lock.txt').read_text()
    assert '# Editable install' not in Path(
        'requirements.dev.lock.txt').read_text()
    assert result.exit_code == 0


@pytest.mark.parametrize('create_setup_py', [True, False])
@pytest.mark.parametrize('create_dev_lock', [True, False])
def test_install_lock_pip(tmp_directory, mock_cmdr_wrapped, create_setup_py,
                          create_dev_lock):
    _write_sample_pip_req('requirements.lock.txt')

    if create_dev_lock:
        _write_sample_pip_req('requirements.dev.lock.txt')

    if create_setup_py:
        Path('setup.py').write_text(setup_py)

    runner = CliRunner()
    result = runner.invoke(install, args='--use-lock', catch_exceptions=False)

    venv, pip = _get_venv_and_pip()

    expected = [
        call('python', '-m', 'venv', venv, description='Creating venv'),
        call(pip,
             'install',
             '--editable',
             '.',
             description='Installing project'),
        call(pip,
             'install',
             '--requirement',
             'requirements.lock.txt',
             description='Installing dependencies'),
        call(pip,
             'install',
             '--requirement',
             'requirements.dev.lock.txt',
             description='Installing dependencies')
    ]

    if not create_setup_py:
        expected.pop(1)

    if not create_dev_lock:
        expected.pop(-1)

    assert mock_cmdr_wrapped.call_args_list == expected
    assert Path('.gitignore').read_text() == f'\n{venv}\n'
    assert result.exit_code == 0


def test_pip_mixed_versions(monkeypatch):
    mock = Mock()
    mock.return_value = """pyflakes==2.4.0\nPygments==2.11.2\n
    pygraphviz @ file:///Users/runner/miniforge3/pygraphviz_1644545996627"""

    monkeypatch.setattr(install_module.Commander, 'run', mock)
    with pytest.warns(UserWarning):
        _pip_install(install_module.Commander, {}, True)
