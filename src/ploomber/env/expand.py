import pydoc
import getpass
from copy import deepcopy
from collections.abc import Mapping
from pathlib import Path

from jinja2 import Template

from ploomber.templates import util
from ploomber import repo


class EnvironmentExpander:
    def __init__(self, raw):
        self.available = self.get_available(raw)
        self.raw = raw

    def __call__(self, value):
        tags = util.get_tags_in_str(value)

        if not tags:
            return value

        params = {k: getattr(self, k)() for k in tags}
        return Template(value).render(**params)

    def get_available(self, raw):
        available = ['user']
        unavailable = {}

        module_name = raw.get('module')

        if module_name:
            module = pydoc.locate(module_name)

            if module is None:
                raise ImportError('Could not import module "{}"'
                                  .format(module_name))

            available.append('version')
            self.module = module
        else:
            unavailable['version'] = RuntimeError('The git_location placeholder is only available if Env defines a "module" constant')

        return available

    def user(self):
        return getpass.getuser()

    def version(self):
        if 'version' in self.available:
            if hasattr(self.module, '__version__'):
                return self.module.__version__
            else:
                raise RuntimeError('Module {} does not have a __version__, cannot '
                                   'expand version placeholder'.format(self.module))
        else:
            raise self.unavailable['version']

    def git(self):
        if 'version' not in self.available:
            raise RuntimeError('The git_location placeholder is only available '
                               'if Env defines a "module" constant')
        else:
            module_path = str(Path(self.module.__file__).parent.absolute())
            return repo.get_env_metadata(module_path)['git_location']


def iterate_nested_dict(d):
    """
    Iterate over all values (possibly nested) in a dictionary
    """
    for k, v in d.items():
        if isinstance(v, Mapping):
            for i in iterate_nested_dict(v):
                yield i
        else:
            yield d, k, v


def mofidy_values(env, modifier):
    env = deepcopy(env)

    for d, k, v in iterate_nested_dict(env):
        d[k] = modifier(v)

    return env


def expand_dict(d):
    return mofidy_values(d, EnvironmentExpander(d))
