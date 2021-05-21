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


def _write_sample_conda_env(name='environment.yml'):
    Path(name).write_text('name: my_tmp_env\ndependencies:\n- pip')


def _write_sample_pip_req(name='requirements.txt'):
    Path(name).touch()


# FIXME: i tested this locally on a windows machine and it works but for some
# reason, the machine running on github actions is unable to locate "conda"
# hence this fails. it's weird because I'm calling conda without issues
# to install dependencies during setup. Same with the next two tests
@pytest.mark.xfail(sys.platform == 'win32',
                   reason='Test not working on Github Actions on Windows')
def test_install_conda(tmp_directory):
    _write_sample_conda_env()
    Path('setup.py').write_text(setup_py)

    runner = CliRunner()
    runner.invoke(install, catch_exceptions=False)

    assert set(os.listdir()) == {
        'environment.yml',
        'environment.lock.yml',
        'sample_package.egg-info',
        'setup.py',
    }


@pytest.mark.xfail(sys.platform == 'win32',
                   reason='Test not working on Github Actions on Windows')
def test_non_package_with_conda(tmp_directory):
    _write_sample_conda_env()
    runner = CliRunner()

    runner.invoke(install, catch_exceptions=False)

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


def test_error_if_env_yml_but_conda_not_installed(monkeypatch):
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
