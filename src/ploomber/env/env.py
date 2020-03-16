"""
Environment management
"""
import logging

from ploomber.env.EnvDict import EnvDict


# TODO: add defaults functionality if defined in {module}/env.defaults.yaml

# TODO: env must be a envdict subclass that can only be instantiated once
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
        * {{git}} expands to branch name if at the tip, otherwise to
        the current commit hash (_module has to be defined)

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
            if not isinstance(source, EnvDict):
                env_dict = EnvDict(source)
            else:
                env_dict = source

            cls._path_to_env = env_dict.path_to_env
            cls._name = env_dict.name
            cls._data = env_dict
            ins = cls()
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
