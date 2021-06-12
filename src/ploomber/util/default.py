"""
Functions to determine defaults

Layouts
-------
TODO: make sure soopervisor documentation also uses these two terms (simple and
packaged)
There are two layouts that can be used, simple and package.

Simple
------
A project composed of a pipeline.yaml and optionally extra pipeline.{name}.yaml
files (all of them must be in the same directory). the parent of pipeline.yaml
is considered the project root and relative sources in all pipeline yaml files
are so to the project root. Pipelines may be parametrized with env.{name}.yaml
files. {{here}} and {{root}} resolve to the same directory.

Packaged
--------
A packaged project has a setup.py file which is considered the project root. It
must contain a src/ directory with a single folder and pipelines are declared
in src/{package-name}/, pipeline.yaml is mandatory, with optional
pipeline.{name}.yaml. env.{name}.yaml can be siblings of pipeline.yaml but
env.yaml files in the current working working directory (which may not be
src/{package-name}) take precedence. {{here}} and {{root}} resolve to
different directories.

TODO: might be confusing for users that on this case, relative paths are so
to the project root (which is setup.py) instead of relative to pipeline.yaml,
which is under src/{package-name}/ how do we make this clear? same could be
say about errors when importing dotted paths (if the dotted path isn't in
full form and does not start with {package-name}). Maybe auto-detect if
we are inside a package and catch FileNotFoundError and ModuleNotFoundError
to provide guidance. e.g., check if the package is properly configured to
help with ModuleNotFoundError and when FileNotFoundError, modify the error
message to suggest using a {{here}} flag or a SourceLoader

TODO: make it clear that packaged projects should be installed with --editable
otherwise there isn't anotion of a setup.py in a parent directory.

TODO: auto-detect when the package is installed in site-packages but pipeline
is executed from local src/*, that should not be allows, recommend installing
in --editable mode

TODO: there is a unique where root_path is undefined: when a packge project
is called from site-packages (e.g ploomber build my_package::pipeline.yaml).
in such case the current working directory is set to be the root path

Default entry point
-------------------
To reduce typing, the command line interface and jupyter search for a
pipeline.yaml (not a pipeline.{name}.yaml) in standard locations. To override
these settings, users may set an environment variable to search for an
alternative file by setting the name (e.g., pipeline.serve.yaml) They have the
option to also use an environment variable to set the default env.{name}.yaml
to use. Note that only the basename is supplied not the path to the file,
since ploomber will look it up in standard locations. Although we could work
with the {name} portion alone (e.g., x in pipeline.x.yaml), we get the full
basename since it's more explicit.
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

    1. ENTRY_POINT environment (ignores root_path and name)
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

    Raises
    ------
    DAGSpecNotFound
        If no pipeline.yaml exists in any of the standard locations
    """
    root_path = root_path or '.'
    env_var = os.environ.get('ENTRY_POINT')

    # env variable gets to priority
    if env_var:
        return env_var

    FILENAME = 'pipeline.yaml' if name is None else f'pipeline.{name}.yaml'

    # try to find a src/*/pipeline.yaml relative to the initial dir
    pkg_location = _package_location(root_path, name=FILENAME)

    relative_to_root_path = Path(root_path, FILENAME)

    # but only return it if there isn't one relative to root dir
    if not relative_to_root_path.exists() and pkg_location:
        return pkg_location

    # look recursively - this will find it relative to root path if it
    # exists

    # we need this bc having a root path is optional
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

    # ALSO look it up relative to root_project? -
    # maybe if root_path is set DO NOT use root_project? from the user's
    # perspective root_project cannot be set so it doesn't make much sense
    # to look in the current directory, or does it?
    # MAYBE also allow in root_project/*/pipeline.yaml?

    # use cases: called by the cli to locate the default entry point to use
    # (called without any arguments)

    # when using DAGSpec.find (calls DAGspec._auto_load), user may supply
    # arguments

    # when deciding whether to add a new scaffold structure or parse the
    # current one and add new files (catches DAGSpecNotFound), called without
    # arguments

    # FIXME: manager.py:115 is also reading ENTRY_POINT
    # when initializing via jupyter (using _auto_load without args)
    # FIXME: jupyter also calls _auto_load with starting dir arg, which should
    # not be the case

    raise DAGSpecNotFound(
        f"""Unable to locate a {FILENAME} at one of the standard locations:

1. A path defined in an ENTRY_POINT environment variable (variable not set)
2. A file relative to {str(root_path)!r} \
(or relative to any of their parent directories)
3. A src/*/{FILENAME} relative to {str(root_path)!r}

Place your {FILENAME} in any of the standard locations or set an ENTRY_POINT
environment variable.
""")


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


def path_to_env(path_to_spec):
    """
    Determines the env.yaml to use. Prefers a file in the local working
    directory, otherwise, one relative to the spec's parent. If the appropriate
    env.yaml file does not exist, returns None. If path to spec has a
    pipeline.{name}.yaml format, it tries to look up an env.{name}.yaml
    first

    Parameters
    ----------
    path_to_spec : str or pathlib.Path
        Path to YAML spec

    Raises
    ------
    ValueError
        If path_to_spec does not have an extension or if it's a directory
    """
    if path_to_spec is not None and Path(path_to_spec).is_dir():
        raise ValueError(
            f'Expected path to spec {str(path_to_spec)!r} to be a '
            'file but got a directory instead')

    if path_to_spec is not None and not Path(path_to_spec).suffix:
        raise ValueError('Expected path to spec to have an extension '
                         f'but got: {str(path_to_spec)!r}')

    path_to_parent = None if path_to_spec is None else Path(
        path_to_spec).parent
    name = None if path_to_parent is None else extract_name(path_to_spec)

    if name is None:
        return path_to_env_with_name(name=None, path_to_parent=path_to_parent)
    else:
        path = path_to_env_with_name(name=name, path_to_parent=path_to_parent)

        if path is None:
            return path_to_env_with_name(name=None,
                                         path_to_parent=path_to_parent)
        else:
            return path


def path_to_env_with_name(name, path_to_parent):
    """Loads an env.yaml file given a parent folder

    It first looks up the PLOOMBER_ENV_FILENAME env var, it if exists, it uses
    the filename defined there. If it doesn't exist, it tries to look for
    env.{name}.yaml, if it doesn't exist, it looks for env.yaml. It returns
    None if None of those files exist, except when PLOOMBER_ENV_FILENAME, in
    such case, it raises an error.

    Raises
    ------
    FileNotFoundError
        If PLOOMBER_ENV_FILENAME is defined but doesn't exist
    ValueError
        If PLOOMBER_ENV_FILENAME is defined and contains a value with
        directories. It must only be a filename
    """
    env_var = os.environ.get('PLOOMBER_ENV_FILENAME')

    if env_var:
        if len(Path(env_var).parts) > 1:
            path_to_parent_abs = str(Path(path_to_parent).resolve())
            raise ValueError(f'PLOOMBER_ENV_FILENAME value ({env_var!r}) '
                             'must be a filename and do not contain any '
                             'directory components (e.g., env.yaml, '
                             'not path/to/env.yaml). Fix the value and place '
                             'the file in the same folder as the YAML '
                             f'spec ({path_to_parent_abs!r}) or next to the '
                             'current working directory')

        filename = env_var
    else:
        filename = 'env.yaml' if name is None else f'env.{name}.yaml'

    local_env = Path('.', filename).resolve()

    if local_env.exists():
        return str(local_env)

    if path_to_parent:
        sibling_env = Path(path_to_parent, filename).resolve()

        if sibling_env.exists():
            return str(sibling_env)

    if env_var:
        raise FileNotFoundError('Failed to load env: PLOOMBER_ENV_FILENAME '
                                f'has value {env_var!r} but '
                                'there isn\'t a file with such name. '
                                'Tried looking it up relative to the '
                                'current working directory '
                                f'({str(local_env)!r}) and relative '
                                f'to the YAML spec ({str(sibling_env)!r})')


def extract_name(path):
    """
    Extract name from a path whose filename is something.{name}.{extension}.
    Returns none if the file doesn't follow the naming convention
    """
    name = Path(path).name
    parts = name.split('.')

    if len(parts) < 3:
        return None
    else:
        return parts[1]


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
    or a setup.py file.

    Parameters
    ---------
    starting_dir : str or pathlib.Path
        The directory to start the search

    raise_ : bool
        Whether to raise an error or not if no root folder is found
    """
    # new logic: go level by level looking for files *at the same time* instead
    # of one first and then the other. only look for pipeline.{name}.yaml and
    # setup.py and stop as you as you find one of them. pass name to know which
    # one to look for (but then we have to make sure all methods that call this
    # also pass the name parameter). if you find a setup.py, look for a
    # src/*/pipeline.{name}.yaml - the main issue is dealing with the name
    # thing, for example, clients make use of this function but they have
    # no context about which pipeline.{name}.yaml they're using
    # open option would be to make pipeline.yaml mandatory when another one
    # with pipeline.{name}.yaml exists
    # also need to check that if setup.py exists,  there must not be a sibling
    # pipeline.yaml, but rather src/*/pipeline.yaml
    # we may also want to check that once we locate the root, there are no
    # other pipeline.{name}.yaml, becuase that will cause trouble since
    # pipeline.yaml is used as root, even for pipeline.{name}.yaml files since
    # clients have no context about this
    # NOTE: this change also requires updateing the docstrings of all clients

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
