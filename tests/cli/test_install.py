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
    version='1.0'
)
"""


# FIXME: i tested this locally on a windows machine and it works but for some
# reason, the machine running on github actions is unable to locale "conda"
# hence this fails. it's weird because I'm calling conda without issues
# to install dependencies during setup
@pytest.mark.xfail(sys.platform == 'win32',
                   reason='Test not working on Github Actions on Windows')
def test_install_conda(tmp_directory):
    Path('environment.yml').write_text(
        'name: my_tmp_env\ndependencies:\n- pip')
    Path('setup.py').write_text(setup_py)

    runner = CliRunner()
    result = runner.invoke(install, catch_exceptions=False)

    assert Path('environment.lock.yml').exists()
    assert Path('environment.dev.lock.yml').exists()
    assert Path('requirements.lock.txt').exists()
    assert Path('requirements.dev.lock.txt').exists()
    assert result.exit_code == 0


def test_install_pip(tmp_directory, monkeypatch):

    mock = Mock(return_value=False)
    monkeypatch.setattr(install_module.shutil, 'which', mock)
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
    assert Path('requirements.dev.lock.txt').exists()
    assert result.exit_code == 0
