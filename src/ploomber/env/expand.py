import re
import ast
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
# difference between an env.yaml and a ploomber.config is clear,
# how to provide a way to initialize this with custom arguments
class EnvironmentExpander:
    #TODO: add version_requires_import=False
    def __init__(self, raw, path_to_env=None):
        self.raw = raw
        self._path_to_env_parent = (None if path_to_env is None
                                    else str(Path(path_to_env).parent))
        # self.version_requires_import = False

        self._placeholders = {}

    def __call__(self, value, parents):
        tags = util.get_tags_in_str(value)

        if not tags:
            return value

        params = {k: self.load_placeholder(k) for k in tags}

        # FIXME: we have duplicated logic here, must only use PathManager
        value = Template(value).render(**params)

        if parents:
            if parents[0] == 'path':
                return Path(value)
            else:
                return value
        else:
            return value

    # TODO: finish this implementation, replace for _module
    def _get_version_import(self, raw):
        package_name = raw.get('_package')

        if not package_name:
            raise KeyError('_package key is required to get version')

        module = pydoc.locate(package_name)

        if module is None:
            raise ImportError('_package "{}" was declared in env but '
                              'import failed'
                              .format(package_name))

        if hasattr(self.module, '__version__'):
            return self.module.__version__
        else:
            raise RuntimeError('Module {} does not have a __version__ '
                               'attribute '.format(self.module))

    def _get_version_without_importing(self):
        if '_module' not in self.raw:
            raise KeyError('_module key is required to use version '
                           'placeholder')

        content = (self.raw['_module'] / '__init__.py').read_text()

        version_re = re.compile(r'__version__\s+=\s+(.*)')

        version = str(ast.literal_eval(version_re.search(
                      content).group(1)))
        return version

    def load_placeholder(self, key):
        if key not in self._placeholders:
            if hasattr(self, 'get_'+key):
                self._placeholders[key] = getattr(self, 'get_'+key)()
            else:
                raise RuntimeError('Unknown placeholder "{}"'.format(key))

        return self._placeholders[key]

    def get_version(self):
        return self._get_version_without_importing()

    def get_user(self):
        return getpass.getuser()

    def get_here(self):
        if self._path_to_env_parent:
            return self._path_to_env_parent
        else:
            raise RuntimeError('here placeholder is only available '
                               'when env was initialized from a file')

    def get_git(self):
        module = self.raw.get('_module')

        if not module:
            raise KeyError('_module key is required to use git placeholder')

        return repo.get_env_metadata(module)['git_location']


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
