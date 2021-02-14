"""
Functions to determine defaults
"""
import os
from glob import glob
from pathlib import Path


def _package_location():
    candidates = sorted([
        f for f in glob('src/*/pipeline.yaml')
        if not str(Path(f).parent).endswith('.egg-info')
    ])

    # FIXME: warn user if more than one
    return candidates[0] if candidates else None


def entry_point():
    """
    Determines default entry point, using the following order:
    1. ENTRY_POINT environment
    2. pipeline.yaml
    3. Package layout default location src/*/pipeline.yaml
    """
    env_var = os.environ.get('ENTRY_POINT')
    pkg_location = _package_location()

    if env_var:
        return env_var
    elif not Path('pipeline.yaml').exists() and pkg_location:
        return pkg_location
    else:
        return 'pipeline.yaml'


def path_to_env(path_to_parent):
    """
    Determines the env.yaml to use

    Parameters
    ----------
    path_to_parent : str or pathlib.Path
        Entry point parent folder
    """
    local_env = Path('.', 'env.yaml').resolve()
    sibling_env = Path(path_to_parent, 'env.yaml').resolve()

    if local_env.exists():
        return str(local_env)
    elif sibling_env.exists():
        return str(sibling_env)
