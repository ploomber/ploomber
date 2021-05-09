import json
import os
import shutil
from pathlib import Path

import yaml

from ploomber.io._commander import Commander


def main():
    if shutil.which('conda'):
        main_conda()
    else:
        main_pip()


def main_pip():
    cmdr = Commander()

    # TODO: modify readme to add how to activate env? probably also in conda
    # TODO: add to gitignore, create if it doesn't exist
    name = Path('.').resolve().name

    venv_dir = f'venv-{name}'
    cmdr.run('python', '-m', 'venv', venv_dir, description='Creating venv')
    cmdr.append_inline(venv_dir, '.gitignore')

    folder = 'Scripts' if os.name == 'nt' else 'bin'
    bin_name = 'pip.EXE' if os.name == 'nt' else 'pip'
    pip = str(Path(venv_dir, folder, bin_name))

    _pip_install_and_lock(cmdr, pip)
    _pip_install_and_lock_dev(cmdr, pip)

    if os.name == 'nt':
        cmd_activate = (
            f'\nIf using cmd.exe: {venv_dir}\\Scripts\\activate.bat'
            f'\nIf using PowerShell: {venv_dir}\\Scripts\\Activate.ps1')
    else:
        cmd_activate = f'source {venv_dir}/bin/activate'

    _next_steps(cmdr, cmd_activate)


def main_conda():
    if not Path('setup.py').exists():
        raise FileNotFoundError(
            '"ploomber install" only works with packaged '
            'projects that have a setup.py file. Use "ploomber scaffold" to'
            ' create one from a template, otherwise use your package manager'
            ' directly to install dependencies')

    if not Path('environment.yml').exists():
        raise FileNotFoundError(
            '"ploomber install" only works with packaged '
            'projects that have a environment.yml file. Use '
            '"ploomber scaffold" to create one from a template, otherwise '
            'use your package manager directly to install dependencies')

    # TODO: ensure ploomber-scaffold includes dependency file (including
    # lock files in MANIFEST.in
    cmdr = Commander()

    # TODO: provide helpful error messages on each command

    with open('environment.yml') as f:
        env_name = yaml.safe_load(f)['name']

    current_env = Path(shutil.which('python')).parents[1].name

    if env_name == current_env:
        raise RuntimeError('environment.yaml will create an environment '
                           f'named {env_name!r}, which is the current active '
                           'environment. Move to a different one and try '
                           'again (e.g., "conda activate base")')

    # get current installed envs
    envs = cmdr.run('conda', 'env', 'list', '--json', capture_output=True)
    already_installed = any([
        env for env in json.loads(envs)['envs']
        # only check in the envs folder, ignore envs in other locations
        if 'envs' in env and env_name in env
    ])

    # if already installed and running on windows, ask to delete first,
    # otherwise it might lead to an intermitent error (permission denied
    # on vcruntime140.dll)
    if already_installed and os.name == 'nt':
        raise ValueError(f'Environemnt {env_name!r} already exists, '
                         f'delete it and try again '
                         f'(conda env remove --name {env_name})')

    pkg_manager = 'mamba' if shutil.which('mamba') else 'conda'
    cmdr.run(pkg_manager,
             'env',
             'create',
             '--file',
             'environment.yml',
             '--force',
             description='Creating env')

    conda_root = Path(shutil.which('conda')).parents[1]
    folder = 'Scripts' if os.name == 'nt' else 'bin'
    bin_name = 'pip.EXE' if os.name == 'nt' else 'pip'
    pip = str(conda_root / 'envs' / env_name / folder / bin_name)

    # this might happen if the environment does not contain python/pip
    if not Path(pip).exists():
        raise FileNotFoundError(
            f'Could not locate pip in environment {env_name!r}, make sure '
            'it is included in your environment.yml and try again')

    _pip_install_and_lock(cmdr, pip)

    env_lock = cmdr.run('conda',
                        'env',
                        'export',
                        '--no-build',
                        '--name',
                        env_name,
                        description='Locking dependencies',
                        capture_output=True)
    Path('environment.lock.yml').write_text(env_lock)

    _pip_install_and_lock_dev(cmdr, pip)

    env_lock_dev = cmdr.run('conda',
                            'env',
                            'export',
                            '--no-build',
                            '--name',
                            env_name,
                            description='Locking dev dependencies',
                            capture_output=True)
    Path('environment.dev.lock.yml').write_text(env_lock_dev)

    cmd_activate = f'conda activate {env_name}'
    _next_steps(cmdr, cmd_activate)


def _pip_install_and_lock(cmdr, pip):
    cmdr.run(pip,
             'install',
             '--editable',
             '.',
             description='Installing project')

    pip_lock = cmdr.run(pip,
                        'freeze',
                        '--exclude-editable',
                        description='Locking dependencies',
                        capture_output=True)
    Path('requirements.lock.txt').write_text(pip_lock)


def _pip_install_and_lock_dev(cmdr, pip):
    cmdr.run(pip,
             'install',
             '--editable',
             '.[dev]',
             description='Installing dev dependencies')

    pip_lock_dev = cmdr.run(pip,
                            'freeze',
                            '--exclude-editable',
                            description='Locking dev dependencies',
                            capture_output=True)
    Path('requirements.dev.lock.txt').write_text(pip_lock_dev)


def _next_steps(cmdr, cmd_activate):
    cmdr.success('Done')
    cmdr.print((f'Next steps:\n1. Activate environment: {cmd_activate}\n'
                '2. Run pipeline: ploomber build'))
    cmdr.success()
