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
        self.expander = expander_class(self._raw_data)
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
        raw = yaml.load(f, Loader=yaml.SafeLoader)

    path = Path(source).resolve()

    return raw, str(path), _get_name(path)


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
