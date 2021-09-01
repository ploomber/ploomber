import json
import os
import shutil
from pathlib import Path

import yaml
from click import exceptions

from ploomber.io._commander import Commander

_SETUP_PY = 'setup.py'

_REQS_LOCK_TXT = 'requirements.lock.txt'
_REQS_TXT = 'requirements.txt'

_ENV_YML = 'environment.yml'
_ENV_LOCK_YML = 'environment.lock.yml'


def main(use_lock):
    """
    Install project, automatically detecting if it's a conda-based or pip-based
    project.

    Parameters
    ---------
    use_lock : bool
        If True Uses requirements.lock.txt/environment.lock.yml and
        requirements.dev.lock.txt/environment.dev.lock.yml files. Otherwise
        it uses regular files and creates the lock ones after installing
        dependencies
    """
    if Path(_ENV_YML).exists() or Path(_ENV_LOCK_YML).exists():

        if not shutil.which('conda'):
            raise exceptions.ClickException('Found environment.yml file but '
                                            'conda is not installed. Install '
                                            'it and try again')

        if use_lock and not Path(_ENV_LOCK_YML).exists():
            raise exceptions.ClickException(
                'Expected an environment.lock.yml due to the '
                '--use-lock/-l option. Run "ploomber install" to create '
                'an environment.lock.yml from an environment.yml')

        main_conda(use_lock)
    elif Path(_REQS_TXT).exists() or Path(_REQS_LOCK_TXT).exists():

        if use_lock and not Path(_REQS_LOCK_TXT).exists():
            raise exceptions.ClickException(
                'Expected a requirements.lock.txt due to the '
                '--use-lock/-l option. Run "ploomber install" to create '
                'a requirements.lock.txt from a requirements.txt')

        main_pip(use_lock)
    else:
        raise exceptions.ClickException(
            'Expected a conda environment.yml or '
            'pip requirements.txt file, but none of those exist. '
            'Add one and try again. You can generate a sample project using '
            'the "ploomber scaffold" command')


def main_pip(use_lock):
    """
    Install pip-based project (uses venv), looks for requirements.txt files

    Parameters
    ----------
    use_lock : bool
        If True Uses requirements.txt and requirements.dev.lock.txt files
    """
    reqs_txt = _REQS_LOCK_TXT if use_lock else _REQS_TXT
    reqs_dev_txt = ('requirements.dev.lock.txt'
                    if use_lock else 'requirements.dev.txt')

    cmdr = Commander()

    # TODO: modify readme to add how to activate env? probably also in conda
    # TODO: add to gitignore, create if it doesn't exist
    name = Path('.').resolve().name

    venv_dir = f'venv-{name}'
    cmdr.run('python', '-m', 'venv', venv_dir, description='Creating venv')
    cmdr.append_inline(venv_dir, '.gitignore')

    folder, bin_name = _get_pip_folder_and_bin_name()
    pip = str(Path(venv_dir, folder, bin_name))

    if Path(_SETUP_PY).exists():
        _pip_install_setup_py_pip(cmdr, pip)

    _pip_install(cmdr, pip, lock=not use_lock, requirements=reqs_txt)

    if Path(reqs_dev_txt).exists():
        _pip_install(cmdr, pip, lock=not use_lock, requirements=reqs_dev_txt)

    if os.name == 'nt':
        cmd_activate = (
            f'\nIf using cmd.exe: {venv_dir}\\Scripts\\activate.bat'
            f'\nIf using PowerShell: {venv_dir}\\Scripts\\Activate.ps1')
    else:
        cmd_activate = f'source {venv_dir}/bin/activate'

    _next_steps(cmdr, cmd_activate)


def main_conda(use_lock):
    """
    Install conda-based project, looks for environment.yml files

    Parameters
    ----------
    use_lock : bool
        If True Uses environment.lock.yml and environment.dev.lock.yml files
    """
    env_yml = _ENV_LOCK_YML if use_lock else _ENV_YML

    # TODO: ensure ploomber-scaffold includes dependency file (including
    # lock files in MANIFEST.in
    cmdr = Commander()

    # TODO: provide helpful error messages on each command

    with open(env_yml) as f:
        env_name = yaml.safe_load(f)['name']

    current_env = Path(shutil.which('python')).parents[1].name

    if env_name == current_env:
        raise RuntimeError(f'{env_yml} will create an environment '
                           f'named {env_name!r}, which is the current active '
                           'environment. Move to a different one and try '
                           'again (e.g., "conda activate base")')

    # get current installed envs
    conda = shutil.which('conda')
    mamba = shutil.which('mamba')

    # if already installed and running on windows, ask to delete first,
    # otherwise it might lead to an intermitent error (permission denied
    # on vcruntime140.dll)
    if os.name == 'nt':
        envs = cmdr.run(conda, 'env', 'list', '--json', capture_output=True)
        already_installed = any([
            env for env in json.loads(envs)['envs']
            # only check in the envs folder, ignore envs in other locations
            if 'envs' in env and env_name in env
        ])

        if already_installed:
            raise ValueError(f'Environment {env_name!r} already exists, '
                             f'delete it and try again '
                             f'(conda env remove --name {env_name})')

    pkg_manager = mamba if mamba else conda
    cmdr.run(pkg_manager,
             'env',
             'create',
             '--file',
             env_yml,
             '--force',
             description='Creating env')

    if Path(_SETUP_PY).exists():
        _pip_install_setup_py_conda(cmdr, env_name)

    if not use_lock:
        env_lock = cmdr.run(conda,
                            'env',
                            'export',
                            '--no-build',
                            '--name',
                            env_name,
                            description='Locking dependencies',
                            capture_output=True)
        Path(_ENV_LOCK_YML).write_text(env_lock)

    _try_conda_install_and_lock_dev(cmdr,
                                    pkg_manager,
                                    env_name,
                                    use_lock=use_lock)

    cmd_activate = f'conda activate {env_name}'
    _next_steps(cmdr, cmd_activate)


def _get_pip_folder_and_bin_name():
    folder = 'Scripts' if os.name == 'nt' else 'bin'
    bin_name = 'pip.exe' if os.name == 'nt' else 'pip'
    return folder, bin_name


def _find_conda_root(conda_bin):
    conda_bin = Path(conda_bin)

    for parent in conda_bin.parents:
        # I've seen variations of this. on windows: Miniconda3 and miniconda3
        # on linux miniconda3 and miniconda
        if parent.name.lower() in {'miniconda3', 'miniconda'}:
            return parent

    raise RuntimeError(
        'Failed to locate conda root from '
        f'directory: {str(conda_bin)!r}. Please submit an issue: '
        'https://github.com/ploomber/ploomber/issues/new')


def _path_to_pip_in_env_with_name(conda_bin, env_name):
    conda_root = _find_conda_root(conda_bin)
    folder, bin_name = _get_pip_folder_and_bin_name()
    return str(conda_root / 'envs' / env_name / folder / bin_name)


def _locate_pip_inside_conda(env_name):
    """
    Locates pip inside the conda env with a given name
    """
    pip = _path_to_pip_in_env_with_name(shutil.which('conda'), env_name)

    # this might happen if the environment does not contain python/pip
    if not Path(pip).exists():
        raise FileNotFoundError(
            f'Could not locate pip in environment {env_name!r}, make sure '
            'it is included in your environment.yml and try again')

    return pip


def _pip_install_setup_py_conda(cmdr, env_name):
    """
    Call "pip install --editable ." if setup.py exists. Automatically locates
    the appropriate pip binary inside the conda env given the env name
    """
    pip = _locate_pip_inside_conda(env_name)
    _pip_install_setup_py_pip(cmdr, pip)


def _pip_install_setup_py_pip(cmdr, pip):
    cmdr.run(pip,
             'install',
             '--editable',
             '.',
             description='Installing project')


def _try_conda_install_and_lock_dev(cmdr, pkg_manager, env_name, use_lock):
    env_yml = 'environment.dev.lock.yml' if use_lock else 'environment.dev.yml'

    if Path(env_yml).exists():
        cmdr.run(pkg_manager,
                 'env',
                 'update',
                 '--file',
                 env_yml,
                 description='Installing dev dependencies')

        if not use_lock:
            env_lock = cmdr.run(shutil.which('conda'),
                                'env',
                                'export',
                                '--no-build',
                                '--name',
                                env_name,
                                description='Locking dev dependencies',
                                capture_output=True)
            Path('environment.dev.lock.yml').write_text(env_lock)


def _next_steps(cmdr, cmd_activate):
    cmdr.success('Done')
    cmdr.print((f'Next steps:\n1. Activate environment: {cmd_activate}\n'
                '2. Run pipeline: ploomber build'))
    cmdr.success()


def _pip_install(cmdr, pip, lock, requirements=_REQS_TXT):
    cmdr.run(pip,
             'install',
             '--requirement',
             requirements,
             description='Installing dependencies')

    if lock:
        pip_lock = cmdr.run(pip,
                            'freeze',
                            '--exclude-editable',
                            description='Locking dependencies',
                            capture_output=True)

        name = Path(requirements).stem
        Path(f'{name}.lock.txt').write_text(pip_lock)
