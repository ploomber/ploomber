import os
import shutil
from pathlib import Path

import yaml

from ploomber.io._commander import Commander


def main():
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

    pkg_manager = 'mamba' if shutil.which('mamba') else 'conda'
    cmdr.run(f'{pkg_manager} env create environment.yml --force', 'Create env')

    conda_root = Path(shutil.which('conda')).parents[1]
    folder = 'Scripts' if os.name == 'nt' else 'bin'
    bin_name = 'pip.EXE' if os.name == 'nt' else 'pip'
    pip = str(conda_root / 'envs' / env_name / folder / bin_name)

    # this might happen if the environment does not contain python/pip
    if not Path(pip).exists():
        raise FileNotFoundError(
            f'Could not locate pip in environment {env_name!r}, make sure '
            'it is included in your environment.yml and try again')

    cmdr.run(f'{pip} install --editable .', 'Install package')

    env_lock = cmdr.run(f'conda env export --no-build --name {env_name}',
                        'Locking dependencies',
                        capture_output=True)
    Path('environment.lock.yml').write_text(env_lock)

    pip_lock = cmdr.run(f'{pip} freeze --exclude-editable',
                        'Locking dependencies',
                        capture_output=True)
    Path('requirements.lock.txt').write_text(pip_lock)

    cmdr.run(f'{pip} install --editable .[dev]', 'Installing dev dependencies')

    env_lock_dev = cmdr.run(f'conda env export --no-build --name {env_name}',
                            'Locking dev dependencies',
                            capture_output=True)
    Path('environment.dev.lock.yml').write_text(env_lock_dev)

    pip_lock_dev = cmdr.run(f'{pip} freeze --exclude-editable',
                            'Locking dev dependencies',
                            capture_output=True)
    Path('requirements.dev.lock.txt').write_text(pip_lock_dev)

    cmdr.tw.sep('=', 'Done', green=True)
    cmdr.tw.write(f'Activate environment: conda activate {env_name}\n')
