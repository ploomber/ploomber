import os
import sys
from pathlib import Path
from unittest.mock import Mock

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
def mock_cmdr(monkeypatch):
    mock = Mock(wraps=install_module.Commander().run)
    monkeypatch.setattr(install_module.Commander, 'run', mock)
    return mock


def _write_sample_conda_env(name='environment.yml'):
    Path(name).write_text('name: my_tmp_env\ndependencies:\n- pip')


def _write_sample_pip_req(name='requirements.txt'):
    Path(name).touch()


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
def test_install_package_conda(tmp_directory, mock_cmdr):
    _write_sample_conda_env()
    Path('setup.py').write_text(setup_py)

    runner = CliRunner()
    runner.invoke(install, catch_exceptions=False)

    # check it calls "pip install --editable ."
    assert mock_cmdr.call_args_list[-2][1][
        'description'] == 'Installing project'

    # check first argument is the path to the conda binary instead of just
    # "conda" since we discovered that fails sometimes on Windows
    assert all([Path(c[0][0]).is_file() for c in mock_cmdr.call_args_list])

    assert set(os.listdir()) == {
        'environment.yml',
        'environment.lock.yml',
        'sample_package.egg-info',
        'setup.py',
    }


@pytest.mark.xfail(sys.platform == 'win32',
                   reason='Test not working on Github Actions on Windows')
def test_install_non_package_with_conda(tmp_directory, monkeypatch, mock_cmdr):
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
    assert all([Path(c[0][0]).is_file() for c in mock_cmdr.call_args_list])

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


def test_conda_error_missing_env_and_reqs(tmp_directory):
    runner = CliRunner()

    result = runner.invoke(install, catch_exceptions=False)
    assert 'Expected a' in result.stdout


def test_error_if_env_yml_but_conda_not_installed(tmp_directory, monkeypatch):
    _write_sample_conda_env()
    runner = CliRunner()
    mock = Mock(return_value=False)
    monkeypatch.setattr(install_module.shutil, 'which', mock)

    result = runner.invoke(install, catch_exceptions=False)
    assert 'Found environment.yml file' in result.stdout


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
