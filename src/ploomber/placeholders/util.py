from jinja2 import Environment, meta


def get_tags_in_str(s):
    """
    Returns tags (e.g. {{variable}}) in a given string as a set, returns an
    empty set for None
    """
    # NOTE: this will not work if the environment used to load
    # the template changes the tags ({{ and }} by default)
    env = Environment()

    # this accepts None and does not break!
    ast = env.parse(s)
    return meta.find_undeclared_variables(ast)
