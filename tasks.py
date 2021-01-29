"""
Setup tasks (requires invoke: pip install invoke)
"""
from pathlib import Path
import base64
from invoke import task
import versioneer


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
    Setup dev environment, requires conda
    """
    if doc and version:
        raise ValueError('doc and version options are incompatible, '
                         'installing docs will install python 3.8')

    version = version or '3.8'
    suffix = '' if version == '3.8' else version.replace('.', '')
    env_name = f'ploomber{suffix}'

    c.run(f'conda create --name {env_name} python={version} --yes')
    c.run('eval "$(conda shell.bash hook)" '
          f'&& conda activate {env_name} '
          '&& conda install pygraphviz r-base r-irkernel --yes '
          '--channel conda-forge'
          '&& pip install --editable .[dev] '
          '&& bash install_test_pkg.sh '
          '&& pip install invoke')

    if doc:
        with c.cd('doc'):
            c.run(
                'eval "$(conda shell.bash hook)" '
                f'&& conda activate {env_name} '
                f'&& conda env update --file environment.yml --name {env_name}'
            )

    print(f'Done! Activate your environment with:\nconda activate {env_name}')


@task
def new(c):
    """Release a new version
    """
    versioneer.release(project_root='.', tag=True)


@task
def upload(c, tag, production=True):
    """Upload to PyPI
    """
    versioneer.upload(tag, production=production)


@task
def test(c, report=False):
    """Run tests
    """
    c.run('pytest --cov ploomber ' + ('--cov-report html' if report else ''),
          pty=True)
