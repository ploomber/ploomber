from itertools import chain
from jinja2 import Environment
from jinja2.nodes import Getattr, Getitem, Assign

from ploomber.placeholders.Placeholder import Placeholder


class JinjaExtractor:
    """Class for parsing jinja templates

    Parameters
    ----------
    code : str or Placeholder
        SQL code
    """
    def __init__(self, code):
        if not isinstance(code, (str, Placeholder)):
            raise TypeError('Code must be a str or Placeholder object, got: '
                            '{}'.format(type(code)))
        self.code = code
        self.ast = self._get_ast(code)

    def _get_ast(self, code):
        if isinstance(code, str):
            env = Environment()
            return env.parse(code)
        else:
            # placeholder
            env = code._template.environment
            return env.parse(code._raw)

    def get_code_as_str(self):
        if isinstance(self.code, str):
            return self.code
        else:
            return self.code._raw

    def find_variable_access(self, variable):
        """Find occurrences of {{variable.something}} and {{variable['something']}}
        """
        attr = self.ast.find_all(Getattr)
        item = self.ast.find_all(Getitem)
        return set([
            obj.arg.as_const() if isinstance(obj, Getitem) else obj.attr
            for obj in chain(attr, item) if obj.node.name == variable
        ]) or None

    def find_variable_assignment(self, variable):
        """
        Find a variable assignment: {% set variable = something %}, returns
        the node that assigns the variable
        """
        variables = {n.target.name: n.node for n in self.ast.find_all(Assign)}
        return None if variable not in variables else variables[variable]
