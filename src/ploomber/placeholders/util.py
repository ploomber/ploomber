from jinja2 import Environment, meta
from jinja2.nodes import Assign

from ploomber.placeholders import extensions

extensions = (extensions.RaiseExtension, )
env_render = Environment(extensions=extensions)
env_runtime = Environment(extensions=extensions,
                          variable_start_string='[[',
                          variable_end_string=']]')


def get_tags_in_str(s):
    """
    Returns tags (e.g. {{variable}}) in a given string as a set, returns an
    empty set for None
    """
    # render placeholders
    vars_render = meta.find_undeclared_variables(env_render.parse(s))

    # runtime placeholders
    vars_runtime = meta.find_undeclared_variables(env_runtime.parse(s))

    return vars_render | vars_runtime


def get_defined_variables(s):
    env = Environment()
    ast = env.parse(s)
    return {n.target.name: n.node.as_const() for n in ast.find_all(Assign)}
