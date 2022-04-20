from pathlib import Path
import subprocess
from ploomber import Env
from ploomber.env.expand import EnvironmentExpander

import pytest

# TODO: just use tmp directory and add file in a fixture


def test_get_version(tmp_directory, cleanup_env):
    Path('__init__.py').write_text('__version__ = "0.1dev0"')
    Path('env.yaml').write_text('_module: .\nversion: "{{version}}"')
    env = Env()
    assert env.version == '0.1dev0'


def test_get_git(tmp_directory, cleanup_env):
    Path('__init__.py').write_text('__version__ = "0.1dev0"')
    Path('env.yaml').write_text('_module: .\ngit: "{{git}}"')

    subprocess.run(['git', 'init'])
    subprocess.run(['git', 'config', 'user.email', 'ci@ploomberio'])
    subprocess.run(['git', 'config', 'user.name', 'Ploomber'])
    subprocess.run(['git', 'add', '--all'])
    subprocess.run(['git', 'commit', '-m', 'first commit'])

    env = Env()
    assert env.git == 'master'


def test_error_on_unknown_placeholder():
    expander = EnvironmentExpander({})

    with pytest.raises(BaseException) as excinfo:
        expander.expand_raw_value('{{unknown}}', parents=[])

    expected = ("Error resolving env: "
                "Undeclared value for placeholder 'unknown'")
    assert expected == str(excinfo.value)


def test_error_on_git_placeholder_if_missing_underscore_module():
    expander = EnvironmentExpander({})

    with pytest.raises(BaseException) as excinfo:
        expander.expand_raw_value('{{git}}', parents=[])

    assert ("could not locate a git repository" in str(excinfo.value))
