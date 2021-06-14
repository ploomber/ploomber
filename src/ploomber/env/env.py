"""
Environment management
"""
from ploomber.env.envdict import EnvDict

# TODO: add defaults functionality if defined in {module}/env.defaults.yaml


class Env:
    """Return the current environment

    NOTE: this API is experimental and subject to change, it is recommmended
    to use @with_env and @load_env decorators instead

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
    >>> Env({'db': {'uri': 'my_uri'}, 'path': {'raw': '/path/to/raw'}})
    >>> env = Env.load()
    >>> env.db.uri # traverse the yaml tree structure using dot notation
    >>> env.path.raw # returns an absolute path to the raw data

    Notes
    -----
    Envs are intended to be short-lived, the recommended usage is to start and
    end them only during the execution of a function that builds a DAG by
    using the @with_env and @load_env decorators
    """
    __instance = None

    # just a variable to display in error messages so users know where
    # the env was initialized if they try to create a new one

    def __new__(cls, source=None):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
            return cls.__instance
        else:
            raise RuntimeError('Cannot start environment, one has already '
                               'started: {}'.format(repr(cls.__instance)))

    def __init__(self, source='env.yaml'):
        """Start the environment

        Parameters
        ----------
        source: dict, pathlib.Path, str, optional
            If dict, loads it directly, if pathlib.Path or path, reads the file
            (assumes yaml format).

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
        if not isinstance(source, EnvDict):
            # try to initialize an EnvDict to perform validation, if any
            # errors occur, discard object
            try:
                source = EnvDict(source)
            except Exception:
                Env.__instance = None
                raise

        self._data = source
        self._fn_name = None

    @classmethod
    def _init_from_decorator(cls, source, fn_name):
        env = Env(source=source)
        env._fn_name = fn_name
        return env

    @classmethod
    def load(cls):
        if cls.__instance is None:
            raise RuntimeError('Env has not been set, run Env() before '
                               'running Env.load()')
        return cls.__instance

    @classmethod
    def end(cls):
        """
        End environment. Usage is discouraged, a single environment is expected
        to exist during the entire Python process lifespan to avoid
        inconsistencies, use it only if you have a very strong reason to
        """
        cls.__instance = None

    def __str__(self):
        return str(self._data)

    def __repr__(self):
        s = 'Env({})'.format(str(self._data))

        if self._fn_name:
            s += ' (initialized in function: %s)' % self._fn_name

        if self._data.path_to_env:
            s += ' (from file: %s)' % str(self._data.path_to_env)

        return s

    def __dir__(self):
        return dir(self._data)

    @property
    def name(self):
        return self._data.name

    def __getattr__(self, key):
        return getattr(self._data, key)

    def __getitem__(self, key):
        return self._data[key]

    def __setattr__(self, name, value):
        if name.startswith('_'):
            super().__setattr__(name, value)
        else:
            raise RuntimeError('env is a read-only object')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        Env.end()
