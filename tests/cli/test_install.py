import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

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
def test_install(tmp_directory):
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
