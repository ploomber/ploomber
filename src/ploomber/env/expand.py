import getpass
from copy import deepcopy
from collections.abc import Mapping

from jinja2 import Template

from ploomber.templates import util


class EnvironmentExpander:

    def __call__(self, value):
        tags = util.get_tags_in_str(value)
        params = {k: getattr(self, k)() for k in tags}
        return Template(value).render(**params)

    def user(self):
        return getpass.getuser()


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
    return mofidy_values(d, EnvironmentExpander())
