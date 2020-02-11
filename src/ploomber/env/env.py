"""
Environment management
"""
import pydoc
import logging
from itertools import chain
from pathlib import Path
from glob import iglob
from io import StringIO
import getpass
import platform
from functools import partial
from inspect import getfullargspec
from collections.abc import Mapping

from ploomber.FrozenJSON import FrozenJSON
from ploomber.path import PathManager
from ploomber import repo

import yaml
from jinja2 import Template


# TODO: add defaults functionality if defined in {module}/env.defaults.yaml
# TODO: improve str(Env) and repr(Env)
# TODO: add suppot for custom placeholders by subclassing, maybe by adding
# get_{placeholder_name} class methods

def load_env(fn):
    """
    A function decorated with @load_env will be called with the current
    environment in an env keyword argument
    """
    args = getfullargspec(fn).args
    kwonlyargs = getfullargspec(fn).kwonlyargs
    args_all = args + kwonlyargs

    if 'env' not in args_all:
        raise TypeError('callable "{}" does not have arg "env"'
                        .format(fn.__name__))

    return partial(fn, env=Env())


class Env:
    """Return the current environment

    Env provides a clean and consistent way of managing environment and
    configuration settings. Its simplest usage provides access to settings
    specified via an `env.yaml`.

    Settings managed by Env are intended to be runtime constant (they are NOT
    intended to be used as global variables). For example you might want
    to store database URIs. Storing sensitive information is discouraged as
    yaml files are plain text. Use `keyring` for that instead.

    All sections are optional, but if there is a path section, all values
    inside that section will be casted to pathlib.Path objects, expanduser()
    is applied so "~" can be used. Strings with a trailing "/" will be
    interpreted as directories and they will be created if they do not exist

    There are a few placeholders available: {{user}} expands to the current
    user (by calling getpass.getuser())

    Examples
    --------
    >>> from ploomber import Env
    >>> Env.start({'db': {'uri': 'my_uri'}, 'path': {'raw': '/path/to/raw'}})
    >>> env = Env()
    >>> env.db.uri # traverse the yaml tree structure using dot notation
    >>> env.path.raw # returns an absolute path to the raw data
    """

    # There are two wildcards available "{{user}}" (returns the current user)
    # and "{{git_location}}" (if the env.yaml file has a "module" key, then
    # # TODO: edit this
    # ... this will return the current branch name, if in detached HEAD
    # state, it will return the hash to the current commit

    # Notes
    # -----
    # The decision of making Env a process-wide instance is to avoid having
    # multiple env instances with different values that would cause calls to
    # env.some_parameter yield different values. Since configuration parameters
    # in Env objects are meant to be constants (such as db URIs) there should not
    # be a need for multiple Envs to exist at any given time. While it is
    # possible to switch to a different Env in the same process, this is
    # discouraged since that could lead to subtle bugs if modules set variables
    # from Env parameters

    _data = None

    @classmethod
    def start(cls, source=None):
        """Start the environment

        Parameters
        ----------
        source: dict, str or pathlib.Path, optional
            Environment source. If str or pathlib.Path, assumes the file
            is in yaml format. If None, it tries to automatically find a
            file by looking at the current working directory and by going to
            parent directories, it first looks for a file named env.{host}.yaml
            where host is replaced by the hostname (by calling
            platform.node()), if it fails, it looks for a file called env.yaml,
            if it cannot find it it raises a FileNotFoundError

        Raises
        ------
        FileNotFoundError
            If source is None and an environment file cannot be found
            automatically
        RuntimeError
            If one environment has already started

        Returns
        -------
        ploomber.Env
            An environment object
        """
        if cls._data is None:

            # try to set it if no argument was provided
            if source is None:
                # look for an env.{name}.yaml, if that fails, try env.yaml
                name = platform.node()
                path_found = find_env_w_name(name)

                if path_found is None:
                    # TODO: improve error message, include hostname
                    raise FileNotFoundError('Could not find env.{}.yaml '
                                            'nor env.yaml'.format(name))
                else:
                    source = path_found

            if isinstance(source, (str, Path)):
                cls._path_to_env = Path(source).resolve()
                cls._name = _get_name(cls._path_to_env)
            else:
                cls._path_to_env = None
                cls._name = None

            cls._path = PathManager(cls)
            cls._data = load(source)

            return cls()

        # if an environment has been set...
        else:
            raise RuntimeError('Cannot start environment, one has already '
                               'started: {}'.format(cls()))

    @classmethod
    def end(cls):
        """
        End environment. Usage is discouraged, a single environment is expected
        to exist during the entire Python process lifespan to avoid
        inconsistencies, use it only if you have a very strong reason to
        """
        cls._path_to_env = None
        cls._name = None
        cls._path = None
        cls._data = None

    def __init__(self):
        if self._data is None:
            raise RuntimeError('Env has not been set, run Env.start before '
                               'running Env()')

        self._logger = logging.getLogger(__name__)

    def __repr__(self):
        return f'Env: {self._path_to_env} - {repr(self._data)}'

    def __dir__(self):
        return dir(self._data)

    @property
    def name(self):
        return self._name

    @property
    def path(self):
        """
        path manager, return paths to directories specified in your env
        """
        return self._path

    def __getattr__(self, key):
        return getattr(self._data, key)

    def __getitem__(self, key):
        return self._data[key]

    def __setattr__(self, name, value):
        if name.startswith('_'):
            super().__setattr__(name, value)
        else:
            raise RuntimeError('env is a read-only object')

    # def get_metadata(self):
    #     """Get env metadata such as git hash, last commit timestamp
    #     """
    #     return repo.get_env_metadata(self.path.home)


def load(source):
    if isinstance(source, (str, Path)):
        env_content = Path(source).read_text()

        with StringIO(env_content) as f:
            m = yaml.load(f, Loader=yaml.SafeLoader)

        module_name = m.get('module')
    elif isinstance(source, Mapping):
        env_content = yaml.dump(source)
        module_name = source.get('module')

    # if "module" is defined in the env file, make sure you can import it
    if module_name is None:
        module = None
    else:
        module = pydoc.locate(module_name)

        if module is None:
            raise ImportError('Could not import module "{}"'
                              .format(module_name))

    # at this point if "module" was defined, we have the module object,
    # otherwise both are None

    params = dict(user=getpass.getuser())

    if '{{version}}' in env_content:
        if module_name is None:
            raise RuntimeError('The git_location placeholder is only available '
                               'if Env defines a "module" constant')
        else:
            if hasattr(module, '__version__'):
                params['version'] = module.__version__
            else:
                raise RuntimeError('Module {} does not have a __version__, cannot '
                                   'expand version placeholder'.format(module_name))

    if '{{git_location}}' in env_content:
        if module_name is None:
            raise RuntimeError('The git_location placeholder is only available '
                               'if Env defines a "module" constant')
        else:
            module_path = str(Path(module.__file__).parent.absolute())
            params['git_location'] = (repo
                                      .get_env_metadata(module_path)
                                      ['git_location'])

    s = Template(env_content).render(**params)

    with StringIO(s) as f:
        content = yaml.load(f, Loader=yaml.SafeLoader)

    env = FrozenJSON(content)

    return env


def find_env_w_name(name):
    """Find environment named env.{name}.yaml by going to parent folders
    """
    path = find_env(name='env.{}.yaml'.format(name))

    if path is None:
        return find_env(name='env.yaml')
    else:
        return path


def find_env(name, max_levels_up=6):
    """Find environment by going to the parent folders
    """
    def levels_up(n):
        return chain.from_iterable(iglob('../' * i + '**')
                                   for i in range(n + 1))

    path_to_env = None

    for filename in levels_up(max_levels_up):
        p = Path(filename)

        if p.name == name:
            path_to_env = filename
            break

    return path_to_env


def _get_name(path_to_env):
    """Parse env.{name}.yaml -> name
    """
    filename = str(Path(path_to_env).name)

    err = ValueError('Wrong filename, must be either env.{name}.yaml '
                     'or env.yaml')

    elements = filename.split('.')

    if len(elements) == 2:
        # no name case
        env, _ = elements
        name = 'root'
    elif len(elements) > 2:
        # name
        env = elements[0]
        name = '.'.join(elements[1:-1])
    else:
        raise err

    if env != 'env':
        raise err

    return name
