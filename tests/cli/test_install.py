from pathlib import Path

from click.testing import CliRunner
from ploomber.cli.cli import install

setup_py = """
from setuptools import setup, find_packages

setup(
    name='sample_package',
    version='1.0'
)
"""


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
