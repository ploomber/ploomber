import importlib
from itertools import chain
from glob import iglob
import platform
from pathlib import Path
from collections.abc import Mapping

import yaml

from ploomber.env import validate
from ploomber.env.expand import modify_values, EnvironmentExpander


class EnvDict(Mapping):
    """
    Implements the initialization functionality for Env, except it allows
    to more than one instance to exist, this is used internally to allow
    factory functions introspection without having to create an actual Env
    """
    def __init__(self, source, expander_class=EnvironmentExpander):
        self._raw_data, self._path_to_env, self.name = load_from_source(source)

        raw_preprocess(self._raw_data, self.path_to_env)

        self.expander = expander_class(self._raw_data, self._path_to_env)
        self._data = modify_values(self._raw_data, self.expander)
        validate.env_dict(self._data)

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self):
        for k in self._data:
            yield k

    def __len__(self):
        return len(self._data)

    @property
    def path_to_env(self):
        if self._path_to_env:
            return Path(self._path_to_env)
        else:
            # maybe raise an error?
            return None


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


def load_from_source(source):
    """
    Loads from a dictionary or a YAML and applies preprocesssing to the
    dictionary
    """
    if isinstance(source, Mapping):
        # dictiionary, path, name
        return source, None, None

    elif source is None:
        # look for an env.{name}.yaml, if that fails, try env.yaml
        name = platform.node()
        path_found = find_env_w_name(name)

        if path_found is None:
            raise FileNotFoundError('Tried to initialize environment with '
                                    'None, but automatic '
                                    'file search failed to locate '
                                    'env.{}.yaml nor env.yaml in the '
                                    'current directory nor 6 levels up'
                                    .format(name))
        else:
            source = path_found

    elif isinstance(source, (str, Path)):
        source_found = find_env(source)

        if source_found is None:
            raise FileNotFoundError('Could not find file "{}" in the '
                                    'current working directory nor '
                                    '6 levels up'.format(source))
        else:
            source = source_found

    with open(source) as f:
        try:
            raw = yaml.load(f, Loader=yaml.SafeLoader)
        except Exception as e:
            raise type(e)('yaml.load failed to parse your YAML file '
                          'fix syntax errors and try again') from e

    path = Path(source).resolve()

    return raw, str(path), _get_name(path)


def raw_preprocess(raw, path_to_raw):
    """
    """
    # preprocess an env - expands _module to module path, raises error
    # if invalid, # expands {{here}} if _module: {{here}}
    # update expander, since _module will be a path when we finish this
    # function
    module = raw.get('_module')

    if module:

        if raw['_module'] == '{{here}}':

            if path_to_raw is not None:
                raw['_module'] = path_to_raw.parent
            else:
                raise ValueError('_module cannot be {{here}} if '
                                 'not loaded from a file')
        else:
            try:
                module_spec = importlib.util.find_spec(module)
            except ValueError:
                # raises ValueError if passed "."
                module_spec = None

            if not module_spec:
                path_to_module = Path(module)

                if not (path_to_module.exists()
                        and path_to_module.is_dir()):
                    raise ValueError('Could not resolve _module "{}", '
                                     'failed to import as a module '
                                     'and is not a directory'
                                     .format(module))

            else:
                path_to_module = Path(module_spec.origin).parent

            raw['_module'] = path_to_module


# TODO: move to entry points module
def flatten_dict(d, prefix=''):
    """
    Convert a nested dict: {'a': {'b': 1}} -> {'a__b': 1}
    """
    out = {}

    for k, v in d.items():
        if isinstance(v, Mapping):
            out = {**out, **flatten_dict(v, prefix=prefix + k + '__')}
        else:
            out[prefix+k] = v

    return out
