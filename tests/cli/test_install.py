import os
import sys
from pathlib import Path
from unittest.mock import Mock, call
import shutil

import pytest
from click.testing import CliRunner

from ploomber.cli import install as install_module
from ploomber.cli.cli import install

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
def mock_cmdr(monkeypatch):
    mock = Mock()
    monkeypatch.setattr(install_module.Commander, 'run', mock)
    return mock


def _write_sample_conda_env(name='environment.yml'):
    Path(name).write_text('name: my_tmp_env\ndependencies:\n- pip')


def _write_sample_pip_req(name='requirements.txt'):
    Path(name).touch()


def _get_venv_and_pip():
    name = Path().resolve().name
    venv = f'venv-{name}'
    pip = str(
        Path(venv, 'Scripts' if os.name == 'nt' else 'bin',
             'pip.exe' if os.name == 'nt' else 'pip'))
    return venv, pip


def test_error_missing_env_yml_and_reqs_txt(tmp_directory):
    runner = CliRunner()

    result = runner.invoke(install, catch_exceptions=False)
    assert 'Expected a conda environment.yml or' in result.stdout


def test_error_if_env_yml_but_conda_not_installed(tmp_directory, monkeypatch):
    _write_sample_conda_env()
    runner = CliRunner()
    mock = Mock(return_value=False)
    monkeypatch.setattr(install_module.shutil, 'which', mock)

    result = runner.invoke(install, catch_exceptions=False)
    assert 'Found environment.yml file' in result.stdout


@pytest.mark.parametrize('args', ['--use-lock', '-l'])
def test_error_if_use_lock_but_env_file_missing(tmp_directory, args):
    _write_sample_conda_env()

    runner = CliRunner()
    result = runner.invoke(install, args=args, catch_exceptions=False)
    assert 'Expected an environment.lock.yml' in result.stdout


@pytest.mark.parametrize('args', ['--use-lock', '-l'])
def test_error_if_use_lock_but_reqs_file_missing(tmp_directory, args):
    _write_sample_pip_req()

    runner = CliRunner()
    result = runner.invoke(install, args=args, catch_exceptions=False)
    assert 'Expected a requirements.lock.txt' in result.stdout


@pytest.mark.parametrize('conda_bin, conda_root', [
    [('something', 'Miniconda3', 'conda'), ('something', 'Miniconda3')],
    [('something', 'miniconda3', 'conda'), ('something', 'miniconda3')],
    [('one', 'miniconda3', 'dir', 'conda'), ('one', 'miniconda3')],
    [('one', 'another', 'Miniconda3', 'conda'),
     ('one', 'another', 'Miniconda3')],
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

    assert set(os.listdir()) == {
        'environment.yml',
        'environment.lock.yml',
    }


@pytest.mark.xfail(sys.platform == 'win32',
                   reason='Test not working on Github Actions on Windows')
def test_non_package_with_conda_with_dev_deps(tmp_directory):
    _write_sample_conda_env()
    _write_sample_conda_env('environment.dev.yml')
    runner = CliRunner()

    runner.invoke(install, catch_exceptions=False)

    assert set(os.listdir()) == {
        'environment.yml',
        'environment.lock.yml',
        'environment.dev.yml',
        'environment.dev.lock.yml',
    }


@pytest.mark.parametrize('create_dev_lock', [True, False])
def test_install_lock_non_package_with_conda(
        tmp_directory, monkeypatch, mock_cmdr, pkg_manager,
        error_if_calling_locate_pip_inside_conda, create_dev_lock):
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

    if not create_dev_lock:
        expected.pop(-1)

    assert mock_cmdr.call_args_list == expected
    assert all([Path(c[0][0]).is_file() for c in mock_cmdr.call_args_list])


@pytest.mark.parametrize('create_dev_lock', [True, False])
def test_install_lock_package_with_conda(tmp_directory, monkeypatch, mock_cmdr,
                                         pkg_manager, create_dev_lock):
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

    if not create_dev_lock:
        expected.pop(-1)

    assert mock_cmdr.call_args_list == expected
    assert all([Path(c[0][0]).is_file() for c in mock_cmdr.call_args_list])


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
