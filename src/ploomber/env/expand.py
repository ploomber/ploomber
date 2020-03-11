import pydoc
import getpass
from copy import deepcopy, copy
from collections.abc import Mapping
from pathlib import Path

from jinja2 import Template

from ploomber.templates import util
from ploomber import repo


# NOTE: what would be the best way for users to provide their own expanders?
# this is useful if they want to expand things like passwords, subclassing
# works partially (only decorators will not work since they use ploomber.Env)
# maybe through a ploomber.config file? We just have to be sure that the
# difference between an env.yaml and a ploomber.config is clear
class EnvironmentExpander:
    def __init__(self, raw):
        self.available = self.get_available(raw)
        self.raw = raw

    def __call__(self, value, parents):
        tags = util.get_tags_in_str(value)

        if not tags:
            return value

        params = {k: getattr(self, k)() for k in tags}

        # FIXME: we have duplicated logic here, must only use PathManager
        value = Template(value).render(**params)

        if parents:
            if parents[0] == 'path':
                return Path(value)
            else:
                return value
        else:
            return value

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


def iterate_nested_dict(d, preffix=[]):
    """
    Iterate over all values (possibly nested) in a dictionary

    Yields: dict holding the value, current key, current value, list of keys
    to get to this value
    """
    # TODO: remove preffix, we are not using it
    for k, v in d.items():
        if isinstance(v, Mapping):
            preffix_new = copy(preffix)
            preffix_new.append(k)
            for i in iterate_nested_dict(v, preffix_new):
                yield i
        else:
            yield d, k, v, copy(preffix)


def modify_values(env, modifier):
    env = deepcopy(env)

    for d, current_key, current_val, parent_keys in iterate_nested_dict(env):
        d[current_key] = modifier(current_val, parent_keys)

    return env
