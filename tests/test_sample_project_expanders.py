from pathlib import Path
import subprocess
from ploomber import Env

# TODO: just use tmp directory and add file here


def test_get_version(tmp_directory, cleanup_env):
    Path('__init__.py').write_text('__version__ = "0.1dev0"')
    Path('env.yaml').write_text('_module: .')
    env = Env.start()
    assert env._expander('{{version}}', []) == '0.1dev0'


def test_get_git(tmp_directory, cleanup_env):
    Path('__init__.py').write_text('__version__ = "0.1dev0"')
    Path('env.yaml').write_text('_module: .')

    subprocess.run(['git', 'init'])
    subprocess.run(['git', 'add', '--all'])
    subprocess.run(['git', 'commit', '-m', 'first commit'])

    env = Env.start()
    assert env._expander('{{git}}', []) == 'master'
