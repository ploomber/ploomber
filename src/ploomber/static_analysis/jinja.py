from itertools import chain
from jinja2.nodes import Getattr, Getitem


def find_variable_access(ast, variable):
    """Find occurrences of {{variable.something}} and {{variable['something']}}
    """
    attr = ast.find_all(Getattr)
    item = ast.find_all(Getitem)
    return set([
        obj.arg.as_const() if isinstance(obj, Getitem) else obj.attr
        for obj in chain(attr, item) if obj.node.name == variable
    ]) or None
