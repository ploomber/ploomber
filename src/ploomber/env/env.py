"""
Environment management
"""
import logging
from itertools import chain
from pathlib import Path
from glob import iglob
import platform

from ploomber.FrozenJSON import FrozenJSON
from ploomber.env.PathManager import PathManager
from ploomber.env import validate
from ploomber.env.expand import modify_values, EnvironmentExpander


import yaml


# TODO: add defaults functionality if defined in {module}/env.defaults.yaml


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

    There are a few placeholders available:
        * {{user}} expands to the current user (by calling getpass.getuser())
        * {{version}} expands to module.__version__ if _module is defined
        * {{git}} expands to the git tag or current commit hash if _module is
        defined

    Examples
    --------
    >>> from ploomber import Env
    >>> Env.start({'db': {'uri': 'my_uri'}, 'path': {'raw': '/path/to/raw'}})
    >>> env = Env()
    >>> env.db.uri # traverse the yaml tree structure using dot notation
    >>> env.path.raw # returns an absolute path to the raw data

    Notes
    -----
    Envs are intended to be short-lived, the recommended usage is to start and
    end them only during the execution of a function that builds a DAG by
    using the @with_env and @load_env decorators
    """
    expander_class = EnvironmentExpander

    _data = None

    @classmethod
    def start(cls, source=None):
        """Start the environment

        Parameters
        ----------
        source: dict, pathlib.Path, str, optional
            If dict, loads it directly, if pathlib.Path, reads the file
            (assumes yaml format), if str, looks for a file named that way
            in the current directory and their parents. If None, it first looks
            for a file named env.{host}.yaml where host is replaced by the
            hostname (by calling platform.node()), if it fails, it looks for a
            file called env.yaml

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

            if isinstance(source, str):
                source_found = find_env(source)

                if source_found is None:
                    raise FileNotFoundError('Could not find file "{}" in the '
                                            'current working directory nor '
                                            '6 levels up'.format(source))
                else:
                    source = source_found

            elif source is None:
                # look for an env.{name}.yaml, if that fails, try env.yaml
                name = platform.node()
                path_found = find_env_w_name(name)

                if path_found is None:
                    raise FileNotFoundError('Could not find env.{}.yaml '
                                            'nor env.yaml'.format(name))
                else:
                    source = path_found

            if isinstance(source, (str, Path)):
                cls._path_to_env = Path(source).resolve()
                cls._name = _get_name(cls._path_to_env)
            else:
                # when loaded form dict, there is no name nor path
                cls._path_to_env = None
                cls._name = None

            cls._path = PathManager(cls)

            if isinstance(source, (str, Path)):
                with open(source) as f:
                    source = yaml.load(f, Loader=yaml.SafeLoader)

            expander = cls.expander_class(source)
            source_expanded = modify_values(source, expander)
            validate.env_dict(source_expanded)

            cls._data = FrozenJSON(source_expanded)

            ins = cls()
            ins._expander = expander
            return ins

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

    def __str__(self):
        return str(self._data)

    def __repr__(self):
        s = 'Env({})'.format(str(self._data))
        if self._path_to_env:
            s += 'loaded from ' + self._path_to_env
        return s

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

    # def __enter__(self):
    #     return self

    # def __exit__(self, exc_type, exc_value, traceback):
    #     self.end()

    # def get_metadata(self):
    #     """Get env metadata such as git hash, last commit timestamp
    #     """
    #     return repo.get_env_metadata(self.path.home)


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
