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
import warnings
import os
from glob import glob
from pathlib import Path
from os.path import relpath

from ploomber.exceptions import DAGSpecNotFound


def _package_location(root_path, name='pipeline.yaml'):
    """
    Look for a src/{package-name}/pipeline.yaml. Returns path to package
    """
    pattern = str(Path(root_path, 'src', '*', name))
    candidates = sorted([
        f for f in glob(pattern)
        if not str(Path(f).parent).endswith('.egg-info')
    ])

    # FIXME: warn user if more than one
    return candidates[0] if candidates else None


# NOTE: this is documented in doc/api/cli.rst, changes should also be reflected
# there
# FIXME: re-write docstring
def entry_point(root_path=None, name=None):
    """
    Determines the default entry point. It first looks for the project root. If
    the project isn't a package, it returns project_root/pipeline.{name}.yaml,
    otherwise src/*/pipeline.{name}.yaml. If the ENTRY_POINT environment
    variable is set, it looks for a file with such name
    (e.g., project_root/{ENTRY_POINT}).

    Parameters
    ----------
    root_path, optional
        Root path to look for the entry point. Defaults to the current working
        directory

    name : str, default=None
        If None, searchs for a pipeline.yaml file otherwise for a
        pipeline.{name}.yaml. Must be None if the ENTRY_POINT environment
        variable is set

    Notes
    -----
    CLI calls this functions with root_path=None

    Raises
    ------
    DAGSpecNotFound
        If no pipeline.yaml exists in any of the standard locations
    ValueError
    """
    # FIXME: rename env var used
    root_path = root_path or '.'
    env_var = os.environ.get('ENTRY_POINT')

    # TODO: raise error if env var and name

    if env_var:
        if len(Path(env_var).parts) > 1:
            raise ValueError(f'ENTRY_POINT ({env_var!r}) '
                             'must be a filename and do not contain any '
                             'directory components (e.g., pipeline.yaml, '
                             'not path/to/pipeline.yaml).')

        filename = env_var
    else:
        filename = 'pipeline.yaml' if name is None else f'pipeline.{name}.yaml'

    # FIXME: it's confusing it it fails at this point, maybe raise a chained
    # exception? - example looking for pipeline.train.yaml which exists
    # but pipeline.yaml doesnt
    project_root = find_root_recursively(starting_dir=root_path)

    if Path(project_root, 'setup.py').exists():
        entry_point = _package_location(root_path=project_root, name=filename)

        if entry_point is not None:
            return relpath(entry_point, Path().resolve())

    if Path(project_root, filename).exists():
        # TODO: handle the case where filename isn't a filename but a dotted
        # path
        return relpath(Path(project_root, filename), Path().resolve())

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

    # TODO: include link to guide explaining how project root is determined
    raise DAGSpecNotFound(
        f"""Unable to locate a {filename} at one of the standard locations:

1. A path defined in an ENTRY_POINT environment variable (variable not set)
2. A file relative to {str(root_path)!r} \
(or relative to any of their parent directories)
3. A src/*/{filename} relative to {str(root_path)!r}

Place your {filename} in any of the standard locations or set an ENTRY_POINT
environment variable.
""")


def try_to_find_entry_point():
    try:
        return entry_point(root_path=None, name=None)
    except:
        pass


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


def find_root_recursively(starting_dir=None):
    """
    Finds a project root by looking recursively for pipeline.yaml or a setup.py
    file. Ignores pipeline.yaml if located in src/*/pipeline.yaml.

    Parameters
    ---------
    starting_dir : str or pathlib.Path
        The directory to start the search

    raise_ : bool
        Whether to raise an error or not if no root folder is found

    Raises
    ------
    FileNotFoundError
        If no setup.py/pipeline.yaml is found or if an incorrect folder layout
        exists
    """
    # NOTE: update the docstrings of all clients - we must catch errors
    # from here and explain that if there isn't a project root a value must
    # be passed explicitly
    # TODO: warn if packaged structured but source loader not configured
    # NOTE: warn if more pipelines in parent directories?
    # TODO: check who is calling raise_=False and check how to fix it
    # FIXME: maybe check that once you found pipeline.yaml, there aren't
    # setup.py as children?

    root_by_setup, setup_levels = find_parent_of_file_recursively(
        'setup.py', max_levels_up=6, starting_dir=starting_dir)

    root_by_pipeline, pipeline_levels = find_parent_of_file_recursively(
        'pipeline.yaml',
        max_levels_up=6,
        starting_dir=starting_dir,
    )

    # use found pipeline.yaml if there is no setup.py OR if the pipeline.yaml
    # if closer to the starting_dir than the setup.py. e.g.,
    # project/some/pipeline.yaml vs project/setup.py
    if root_by_pipeline and (not root_by_setup
                             or pipeline_levels < setup_levels):
        others = glob(str(Path(root_by_pipeline, '**', 'pipeline*.yaml')),
                      recursive=True)

        if others:
            others_fmt = ''.join(f'* {other}\n' for other in others)

            warnings.warn(
                'Found other pipeline files in children directories of the '
                f'project root ({str(root_by_pipeline)!r}):\n\n'
                f'{others_fmt}\nThis will cause each pipeline to have its own '
                'project root, which is not recommended. Consider '
                'moving them to the same project root with different names '
                '(e.g., project/pipeline.yaml and '
                'project/pipeline.serve.yaml) or as siblings (e.g., '
                'project1/pipeline.yaml and project2/pipeline.yaml)')

        return root_by_pipeline

    # FIXME: maybe apply the same levels rule?
    if root_by_setup:
        pipeline_yaml = Path(root_by_setup, 'pipeline.yaml')
        pkg_location = _package_location(root_path=root_by_setup)

        if not pkg_location:
            if pipeline_yaml.exists():
                msg = ('. Move the pipeline.yaml file that exists in the '
                       'same folder than setup.py to src/{pkg}/pipeline.yaml '
                       'where pkg is the name of your package.')
            else:
                msg = ''

            raise FileNotFoundError('Failed to determine project root. Found '
                                    'a setup.py file at '
                                    f'{str(root_by_setup)!r} and expected '
                                    'to find a pipeline.yaml file at '
                                    'src/*/pipeline.yaml (relative to '
                                    'setup.py parent) but no such file was '
                                    'found' + msg)
        elif pipeline_yaml.exists():
            pkg = Path(*Path(pkg_location).parts[-3:-1])
            example = str(pkg / 'pipeline.another.yaml')
            raise FileExistsError('Failed to determine project root: found '
                                  f'two pipeline.yaml files: {pkg_location} '
                                  f'and {pipeline_yaml}. To fix it, move '
                                  'and rename the second file '
                                  f'under {str(pkg)} (e.g., {example})')

        return root_by_setup

    raise FileNotFoundError('Failed to determine project root. Looked '
                            'recursively for a setup.py or '
                            'pipeline.yaml in parent folders but none of '
                            'those files exist')


def try_to_find_root_recursively():
    try:
        return find_root_recursively()
    except Exception:
        pass


def find_package_name(starting_dir=None):
    """
    Find package name for this project. Raises an error if it cannot find it
    """
    root = find_root_recursively(starting_dir=starting_dir, raise_=True)
    pkg = _package_location(root_path=root)
    return Path(pkg).parent.name
