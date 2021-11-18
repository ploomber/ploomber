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
otherwise there isn't a notion of a setup.py in a parent directory.

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
import warnings
import os
from glob import glob
from pathlib import Path
from os.path import relpath

from ploomber.exceptions import DAGSpecInvalidError
from ploomber.entrypoint import try_to_find_entry_point_type, EntryPoint


def _package_location(root_path, name='pipeline.yaml'):
    """
    Look for a src/{package-name}/pipeline.yaml relative to root_path

    Parameters
    ----------
    root_path : str or pathlib.Path
        Looks for a package relative to this

    name : str, default='pipeline.yaml'
        YAML spec to search for

    Returns
    -------
    str
        Path to package. None if no package exists.
    """
    pattern = str(Path(root_path, 'src', '*', name))
    candidates = sorted([
        f for f in glob(pattern)
        if not str(Path(f).parent).endswith('.egg-info')
    ])

    if len(candidates) > 1:
        warnings.warn(f'Found more than one package location: {candidates}. '
                      f'Using the first one: {candidates[0]!r}')

    return candidates[0] if candidates else None


def entry_point_with_name(root_path=None, name=None):
    """Search for an entry point with a given name

    Parameters
    ----------
    name : str, default=None
        If None, searchs for a pipeline.yaml file otherwise for a
        file with such name
    """
    filename = name or 'pipeline.yaml'

    # first, find project root
    project_root = find_root_recursively(starting_dir=root_path,
                                         filename=filename)
    setup_py = Path(project_root, 'setup.py')
    setup_py_exists = setup_py.exists()

    # if ther is a setup.py file, look up a {project_root}/src/*/{name} file
    if setup_py_exists:
        entry_point = _package_location(root_path=project_root, name=filename)

        if entry_point is not None:
            return relpath(entry_point, Path().resolve())

    # otherwise use {project_root}/{file}. note that this file must
    # exust since find_root_recursively raises an error if it doesn't
    return relpath(Path(project_root, filename), Path().resolve())


# NOTE: this is described in doc/api/cli.rst. changes here must be also
# documented there
def entry_point(root_path=None):
    """
    Determines the default YAML specentry point. It first determines the
    project root. If the project isn't a package, it returns
    project_root/pipeline.yaml, otherwise src/*/pipeline.yaml. If the
    ENTRY_POINT environment variable is set, it looks for a file with
    such name (e.g., project_root/{ENTRY_POINT}).

    Parameters
    ----------
    root_path, optional
        Root path to look for the entry point. Defaults to the current working
        directory


    Notes
    -----
    Use cases for this function:
    * Called by the cli to locate the default entry point to use (nor args)
    * When deciding whether to add a new scaffold structure or parse the
        current one and add new files (catches DAGSpecInvalidError), no args

    Raises
    ------
    DAGSpecInvalidError
        If fails to determine project root or if no pipeline.yaml (or
        the content of the ENTRY_POINT environment variable, if any) exists in
        the expected location (once project root is determined).
    """
    # FIXME: rename env var used
    root_path = root_path or '.'
    env_var = os.environ.get('ENTRY_POINT')

    if env_var:
        if len(Path(env_var).parts) > 1:
            raise ValueError(f'ENTRY_POINT ({env_var!r}) '
                             'must be a filename and do not contain any '
                             'directory components (e.g., pipeline.yaml, '
                             'not path/to/pipeline.yaml).')

        filename = env_var
    else:
        filename = 'pipeline.yaml'

    return entry_point_with_name(root_path=root_path, name=filename)


def try_to_find_entry_point():
    """Try to find the default entry point. Returns None if it isn't possible
    """
    # check if it's a dotted path

    type_ = try_to_find_entry_point_type(os.environ.get('ENTRY_POINT'))

    if type_ == EntryPoint.DottedPath:
        return os.environ.get('ENTRY_POINT')

    # entry_point searches recursively for a YAML spec

    try:
        return entry_point(root_path=None)
    except Exception:
        # TODO: maybe display a warning with the error?
        pass


def entry_point_relative(name=None):
    """
    Returns a relative path to the entry point with the given name.

    Raises
    ------
    DAGSpecInvalidError
        If cannot locate the requested file

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
        raise DAGSpecInvalidError(
            f'Error loading {FILENAME}, both {location} '
            '(relative to the current working directory) '
            f'and {location_pkg} exist, but expected only one. '
            f'If your project is a package, keep {location_pkg}, '
            f'if it\'s not, keep {location}')

    if location_pkg is None and location is None:
        raise DAGSpecInvalidError(
            f'Could not find dag spec with name {FILENAME}, '
            'make sure the file is located relative to the working directory '
            f'or in src/pkg_name/{FILENAME} (where pkg_name is the name of '
            'your package if your project is one)')

    return location_pkg or location


def path_to_env_from_spec(path_to_spec):
    """

    It first looks up the PLOOMBER_ENV_FILENAME env var, it if exists, it uses
    the filename defined there. If not, it looks for an env.yaml file. Prefers
    a file in the working directory, otherwise, one relative to the spec's
    parent. If the appropriate env.yaml file does not exist, returns None.
    If path to spec has a pipeline.{name}.yaml format, it tries to look up an
    env.{name}.yaml first.

    It returns None if None of those files exist, except when
    PLOOMBER_ENV_FILENAME, in such case, it raises an error.

    Parameters
    ----------
    path_to_spec : str or pathlib.Path
        Path to YAML spec

    Raises
    ------
    FileNotFoundError
        If PLOOMBER_ENV_FILENAME is defined but doesn't exist
    ValueError
        If PLOOMBER_ENV_FILENAME is defined and contains a path with
        directory components.
        If path_to_spec does not have an extension or if it's a directory
    """
    # FIXME: delete this
    if path_to_spec is None:
        return None

    if Path(path_to_spec).is_dir():
        raise ValueError(
            f'Expected path to spec {str(path_to_spec)!r} to be a '
            'file but got a directory instead')

    if not Path(path_to_spec).suffix:
        raise ValueError('Expected path to spec to have a file extension '
                         f'but got: {str(path_to_spec)!r}')

    path_to_parent = Path(path_to_spec).parent
    environ = _get_env_filename_environment_variable(path_to_parent)
    if environ:
        filename = environ
    else:
        name = environ or extract_name(path_to_spec)
        filename = 'env.yaml' if name is None else f'env.{name}.yaml'

    return _search_for_env_with_name_and_parent(filename,
                                                path_to_parent,
                                                raise_=environ is not None)


def _search_for_env_with_name_and_parent(filename, path_to_parent, raise_):
    # pipeline.yaml....
    if filename is None:
        # look for env.yaml...
        return _path_to_filename_in_cwd_or_with_parent(
            filename='env.yaml', path_to_parent=path_to_parent, raise_=raise_)
    # pipeline.{name}.yaml
    else:
        # look for env.{name}.yaml
        path = _path_to_filename_in_cwd_or_with_parent(
            filename=filename, path_to_parent=path_to_parent, raise_=raise_)

        # not found, try with env.yaml...
        if path is None:
            return _path_to_filename_in_cwd_or_with_parent(
                filename='env.yaml',
                path_to_parent=path_to_parent,
                raise_=raise_)
        else:
            return path


def _path_to_filename_in_cwd_or_with_parent(filename, path_to_parent, raise_):
    """
    Looks for a file with filename in the current working directory, if it
    doesn't exist, it looks for it relative to path_to_parent.

    Parameters
    ----------
    filename : str
        Filename to search for

    path_to_parent : str or pathlib.Path
        If filename does not exist in the current working directory, look
        relative to this path

    raise_ : bool
        If Trye, raises an error if the file doesn't exist
    """
    local_env = Path('.', filename).resolve()

    if local_env.exists():
        return str(local_env)

    if path_to_parent:
        sibling_env = Path(path_to_parent, filename).resolve()

        if sibling_env.exists():
            return str(sibling_env)

    if raise_:
        raise FileNotFoundError('Failed to load env: PLOOMBER_ENV_FILENAME '
                                f'has value {filename!r} but '
                                'there isn\'t a file with such name. '
                                'Tried looking it up relative to the '
                                'current working directory '
                                f'({str(local_env)!r}) and relative '
                                f'to the YAML spec ({str(sibling_env)!r})')


def _get_env_filename_environment_variable(path_to_parent):
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

    return env_var


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
    path : str
        Absolute path to the file
    levels : int
        How many levels up the file is located
    """
    current_dir = starting_dir or os.getcwd()
    current_dir = Path(current_dir).resolve()
    path_to_file = None
    levels = None

    for levels in range(max_levels_up):
        current_path = Path(current_dir, name)

        if current_path.exists():
            path_to_file = current_path.resolve()
            break

        current_dir = current_dir.parent

    return path_to_file, levels


def find_parent_of_file_recursively(name, max_levels_up=6, starting_dir=None):
    path, levels = find_file_recursively(name,
                                         max_levels_up=6,
                                         starting_dir=starting_dir)

    if path:
        return path.parent, levels

    return None, None


def find_root_recursively(starting_dir=None,
                          filename=None,
                          check_parents=True):
    """
    Finds a project root by looking recursively for pipeline.yaml or a setup.py
    file. Ignores pipeline.yaml if located in src/*/pipeline.yaml.

    Parameters
    ---------
    starting_dir : str or pathlib.Path
        The directory to start the search


    Raises
    ------
    DAGSpecInvalidError
        If fails to determine a valid project root
    """
    filename = filename or 'pipeline.yaml'

    if len(Path(filename).parts) > 1:
        raise ValueError(f'{filename!r} should be a filename '
                         '(e.g., pipeline.yaml), not a path '
                         '(e.g., path/to/pipeline.yaml)')

    root_by_setup, setup_levels = find_parent_of_file_recursively(
        'setup.py', max_levels_up=6, starting_dir=starting_dir)

    root_by_pipeline, pipeline_levels = find_parent_of_file_recursively(
        filename,
        max_levels_up=6,
        starting_dir=starting_dir,
    )

    root_found = None

    # use found pipeline.yaml if not in a src/*/pipeline.yaml structure when
    # there is no setup.py
    # OR
    # if the pipeline.yaml if closer to the starting_dir than the setup.py.
    # e.g., project/some/pipeline.yaml vs project/setup.py
    if root_by_pipeline and not root_by_setup:
        if root_by_pipeline.parents[0].name == 'src':
            pkg_portion = str(Path(*root_by_pipeline.parts[-2:]))
            raise DAGSpecInvalidError(
                'Invalid project layout. Found project root at '
                f'{root_by_pipeline}  under a parent with name {pkg_portion}. '
                'This suggests a package '
                'structure but no setup.py exists. If your project is a '
                'package create a setup.py file, otherwise rename the '
                'src directory')

        root_found = root_by_pipeline
    elif root_by_pipeline and root_by_setup:
        if (pipeline_levels < setup_levels
                and root_by_pipeline.parents[0].name != 'src'):
            root_found = root_by_pipeline

    if root_by_setup and (not root_by_pipeline
                          or setup_levels <= pipeline_levels
                          or root_by_pipeline.parents[0].name == 'src'):
        pipeline_yaml = Path(root_by_setup, filename)

        # pkg_location checks if there is a src/{package-name}/{filename}
        # e.g., src/my_pkg/pipeline.yaml
        pkg_location = _package_location(root_path=root_by_setup,
                                         name=filename)

        if not pkg_location and not pipeline_yaml.exists():
            raise DAGSpecInvalidError(
                'Failed to determine project root. Found '
                'a setup.py file at '
                f'{str(root_by_setup)!r} and expected '
                f'to find a {filename} file at '
                f'src/*/{filename} (relative to '
                'setup.py parent) but no such file was '
                'found')

        if pkg_location and pipeline_yaml.exists():
            pkg = Path(*Path(pkg_location).parts[-3:-1])
            example = str(pkg / 'pipeline.another.yaml')
            raise DAGSpecInvalidError(
                'Failed to determine project root: found '
                f'two {filename} files: {pkg_location} '
                f'and {pipeline_yaml}. To fix it, move '
                'and rename the second file '
                f'under {str(pkg)} (e.g., {example}) or move {pkg_location} '
                'to your root directory')

        root_found = root_by_setup

    if root_found:
        if check_parents:
            try:
                another_found = find_root_recursively(
                    starting_dir=Path(root_found).parent,
                    filename=filename,
                    check_parents=False)
            except DAGSpecInvalidError:
                pass
            else:
                warnings.warn(
                    f'Found project root with filename {filename!r} '
                    f'at {str(root_found)!r}, but '
                    'found another one in a parent directory '
                    f'({str(another_found)!r}). The former will be used. '
                    'Nested YAML specs are not recommended, '
                    'consider moving them to the same folder and '
                    'rename them (e.g., project/pipeline.yaml '
                    'and project/pipeline.serve.yaml) or store them '
                    'in separate folders (e.g., '
                    'project1/pipeline.yaml and '
                    'project2/pipeline.yaml')

        return root_found
    else:
        raise DAGSpecInvalidError('Failed to determine project root. Looked '
                                  'recursively for a setup.py or '
                                  f'{filename} in parent folders but none of '
                                  'those files exist')


def try_to_find_root_recursively(starting_dir=None):
    # TODO: display warning
    try:
        return find_root_recursively(starting_dir=starting_dir)
    except Exception:
        pass


def find_package_name(starting_dir=None):
    """
    Find package name for this project. Raises an error if it cannot determine
    a valid root path
    """
    root = find_root_recursively(starting_dir=starting_dir)
    pkg = _package_location(root_path=root)

    if not pkg:
        raise ValueError(
            'Could not find a valid package. Make sure '
            'there is a src/package-name/pipeline.yaml file relative '
            f'to your project root ({root})')

    return Path(pkg).parent.name
