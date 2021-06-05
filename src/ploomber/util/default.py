"""
Functions to determine defaults
"""
import os
from glob import glob
from pathlib import Path
from os.path import relpath

from ploomber.exceptions import DAGSpecNotFound


def _package_location(root_path, name='pipeline.yaml'):
    pattern = str(Path(root_path, 'src', '*', name))
    candidates = sorted([
        f for f in glob(pattern)
        if not str(Path(f).parent).endswith('.egg-info')
    ])

    # FIXME: warn user if more than one
    return candidates[0] if candidates else None


# NOTE: this is documented in doc/api/cli.rst, changes should also be reflected
# there
def entry_point(root_path=None, name=None):
    """
    Determines default entry point (relative to root_path),
    using the following order:

    1. ENTRY_POINT environment
    2. {root_path}/pipeline.yaml
    3. Package layout default location src/*/pipeline.yaml
    4. Parent folders of root_path
    5. Looks for a setup.py in the parent folders, then src/*/pipeline.yaml

    Parameters
    ----------
    root_path, optional
        Root path to look for the entry point. Defaults to the current working
        directory

    name : str, default=None
        If None, searchs for a pipeline.yaml file otherwise for a
        pipeline.{name}.yaml


    Notes
    -----
    CLI calls this functions with root_path=None
    """
    FILENAME = 'pipeline.yaml' if name is None else f'pipeline.{name}.yaml'

    root_path = root_path or '.'
    env_var = os.environ.get('ENTRY_POINT')

    # env variable gets to priority
    if env_var:
        return env_var

    # try to find a src/*/pipeline.yaml relative to the initial dir
    pkg_location = _package_location(root_path, name=FILENAME)

    # but only return it if there isn't one relative to root dir
    if not Path(root_path, FILENAME).exists() and pkg_location:
        return pkg_location

    # look recursively
    parent_location = find_file_recursively(FILENAME,
                                            max_levels_up=6,
                                            starting_dir=root_path)

    # if you found it, return it
    if parent_location:
        return relpath(Path(parent_location).resolve(),
                       start=Path(root_path).resolve())

    # the only remaining case is a src/*/pipeline.yaml relative to a parent
    # directory. First, find the project root, then try to look for the
    # src/*/pipeline.yaml
    root_project = find_root_recursively(starting_dir=root_path)

    if root_project:
        pkg_location = _package_location(root_project, name=FILENAME)

        if pkg_location:
            return relpath(Path(pkg_location).resolve(),
                           start=Path(root_path).resolve())

    # FIXME: this should raise a DAGSpecNotFound error
    return FILENAME


def entry_point_relative(name=None):
    """
    Returns a relative path to the entry point with the given name.

    Raises
    ------
    DAGSpecNotFound
        If cannot locate the requested file

    ValueError
        If more than one file with the name exist (i.e., both
        pipeline.{name}.yaml and src/*/pipeline.{name}.yaml)

    Notes
    -----
    This is used by Soopervisor when loading dags. We must ensure it gets
    a relative path since such file is used for loading the spec in the client
    and in the hosted container (which has a different filesystem structure)
    """
    FILENAME = 'pipeline.yaml' if name is None else f'pipeline.{name}.yaml'

    location_pkg = _package_location(root_path='.', name=FILENAME)
    location = FILENAME if Path(FILENAME).exists() else None

    if location_pkg and location:
        raise ValueError(f'Error loading {FILENAME}, both {location} '
                         '(relative to the current working directory) '
                         f'and {location_pkg} exist, but expected only one. '
                         f'If your project is a package, keep {location_pkg}, '
                         f'if it\'s not, keep {location}')

    if location_pkg is None and location is None:
        raise DAGSpecNotFound(
            f'Could not find dag spec with name {FILENAME}, '
            'make sure the file is located relative to the working directory '
            f'or in src/pkg_name/{FILENAME} (where pkg_name is the name of '
            'your package if your project is one)')

    return location_pkg or location


def path_to_env(path_to_parent):
    """
    Determines the env.yaml to use

    Parameters
    ----------
    path_to_parent : str or pathlib.Path
        Entry point parent folder
    """
    local_env = Path('.', 'env.yaml').resolve()

    if local_env.exists():
        return str(local_env)

    if path_to_parent:
        sibling_env = Path(path_to_parent, 'env.yaml').resolve()

        if sibling_env.exists():
            return str(sibling_env)


def find_file_recursively(name, max_levels_up=6, starting_dir=None):
    """
    Find a file by looking into the current folder and parent folders,
    returns None if no file was found otherwise pathlib.Path to the file

    Parameters
    ----------
    name : str
        Filename

    Returns
    -------
    path
        Absolute path to the file
    """
    current_dir = starting_dir or os.getcwd()
    current_dir = Path(current_dir).resolve()
    path_to_file = None

    for _ in range(max_levels_up):
        current_path = Path(current_dir, name)

        if current_path.exists():
            path_to_file = current_path.resolve()
            break

        current_dir = current_dir.parent

    return path_to_file


def find_root_recursively(starting_dir=None, raise_=False):
    """
    Finds a project root by looking recursively for environment files
    or a setup.py file. If None of those files exist, it returns the current
    working directory if there is a pipeline.yaml file

    Parameters
    ---------
    starting_dir : str or pathlib.Path
        The directory to start the search

    raise_ : bool
        Whether to raise an error or not if no root folder is found
    """
    options = [
        'environment.yml',
        'environment.lock.yml',
        'requirements.txt',
        'requirements.lock.txt',
        'setup.py',
    ]

    for name in options:
        path = find_file_recursively(name,
                                     max_levels_up=6,
                                     starting_dir=starting_dir)

        if path:
            return path.parent

    if raise_:
        raise ValueError(
            'Could not determine project\'s root directory. '
            'Looked recursively for an environment.yml, requirements.txt or'
            ' setup.py file. Add one of those and try again.')


def find_package_name(starting_dir=None):
    """
    Find package name for this project. Raises an error if it cannot find it
    """
    root = find_root_recursively(starting_dir=starting_dir, raise_=True)

    pkg = _package_location(root_path=root)

    if not pkg:
        raise ValueError('Could not find a valid package. Make sure '
                         'there is a src/package/pipeline.yaml file relative '
                         f'to your project root ({root})')

    return Path(pkg).parent.name
