from jinja2 import Environment, meta


def get_tags_in_str(s):
    if s is None:
        raise ValueError('Got None')

    # NOTE: this will not work if the environment used to load
    # the template changes the tags ({{ and }} by default)
    env = Environment()

    # this accepts None and does not break!
    ast = env.parse(s)
    return meta.find_undeclared_variables(ast)
