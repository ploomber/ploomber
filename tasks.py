"""
Setup tasks (requires invoke: pip install invoke)
"""
import sys
import platform
from pathlib import Path
import base64
from invoke import task
import shutil

_IS_WINDOWS = platform.system() == 'Windows'
_PY_DEFAULT_VERSION = '3.9'

if not Path('LICENSE').exists():
    sys.exit('Error: Run the command from the root folder (the directory '
             'with the README.md and setup.py files)')


@task
def db_credentials(c):
    """Encode db credentials (for github actions)
    """
    path = str(Path('~', '.auth', 'postgres-ploomber.json').expanduser())
    creds = Path(path).read_text()
    print(base64.b64encode(creds.encode()).decode())


@task
def setup(c, doc=False, version=None):
    """
    [conda] Setup dev environment
    """
    if doc and version:
        raise ValueError('doc and version options are incompatible, '
                         'installing docs will install python 3.8')

    version = version or _PY_DEFAULT_VERSION
    suffix = '' if version == _PY_DEFAULT_VERSION else version.replace('.', '')
    env_name = f'ploomber{suffix}'

    cmds = [
        'eval "$(conda shell.bash hook)"',
        f'conda activate {env_name}',
        'conda install pygraphviz r-base r-irkernel --yes -c conda-forge',
        'pip install --editable .[dev]',
        'pip install --editable tests/assets/test_pkg',
    ]

    if _IS_WINDOWS:
        cmds.pop(0)

    c.run(f'conda create --name {env_name} python={version} --yes')

    c.run(' && '.join(cmds))

    if doc:
        cmds = [
            'eval "$(conda shell.bash hook)"',
            f'conda activate {env_name}'
            f'conda env update --file environment.yml --name {env_name}',
        ]

        if _IS_WINDOWS:
            cmds.pop(0)

        with c.cd('doc'):
            c.run(' && '.join(cmds))

    print(f'Done! Activate your environment with:\nconda activate {env_name}')


@task
def setup_pip(c, doc=False):
    """[pip] Setup dev environment
    """
    # install ploomber in editable mode and include development dependencies
    c.run('pip install --editable ".[dev]"')

    # install sample package required in some tests
    c.run('pip install --editable tests/assets/test_pkg')

    # install doc dependencies
    if doc:
        c.run('pip install -r doc/requirements.txt')

    print('Warning: installing with pip skips some dependencies. '
          'See contributing.md "Setup with pip for details"')


@task
def docs(c):
    """Build docs
    """
    with c.cd('doc'):
        c.run('make html')


@task
def new(c):
    """Release a new version
    """
    from pkgmt import versioneer
    versioneer.version(project_root='.', tag=True)


@task
def upload(c, tag, production=True):
    """Upload to PyPI
    """
    from pkgmt import versioneer
    versioneer.upload(tag, production=production)
    print('Remember to update binder-env!')


@task
def test(c, report=False):
    """Run tests
    """
    c.run('pytest --cov ploomber ' + ('--cov-report html' if report else ''),
          pty=True)
    c.run('flake8')


@task
def install_git_hook(c, force=False):
    """Installs pre-push git hook
    """
    path = Path('.git/hooks/pre-push')
    hook_exists = path.is_file()

    if hook_exists:
        if force:
            path.unlink()
        else:
            sys.exit('Error: pre-push hook already exists. '
                     'Run: "invoke install-git-hook -f" to force overwrite.')

    shutil.copy('.githooks/pre-push', '.git/hooks')
    print(f'pre-push hook installed at {str(path)}')


@task
def uninstall_git_hook(c):
    """Uninstalls pre-push git hook
    """
    path = Path('.git/hooks/pre-push')
    hook_exists = path.is_file()

    if hook_exists:
        path.unlink()
        print(f'Deleted {str(path)}.')
    else:
        print('Hook doesn\'t exist, nothing to delete.')
