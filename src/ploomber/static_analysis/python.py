"""
Extract "upstream" and "product" from Python notebooks
"""
import ast
import parso
from collections.abc import Mapping

from ploomber.static_analysis.abstract import NotebookExtractor, Extractor


class PythonNotebookExtractor(NotebookExtractor):
    def extract_upstream(self):
        return extract_upstream_assign(self.parameters_cell)

    def extract_product(self):
        """
        Extract "product" from a Python code string
        """
        product_found, product = extract_variable(self.parameters_cell,
                                                  'product')

        if not product_found or product is None:
            raise ValueError("Couldn't extract 'product' "
                             "from code:\n%s" % self.parameters_cell)
        else:
            return product


class PythonCallableExtractor(Extractor):
    def extract_upstream(self):
        """
        Extract keys requested to an upstream variable (e.g. upstream['key'])
        """
        module = ast.parse(self.code)
        return {
            node.slice.value.s
            for node in ast.walk(module)
            if isinstance(node, ast.Subscript) and node.value.id == 'upstream'
            and isinstance(node.slice.value, ast.Str)
        } or None

    def extract_product(self):
        raise NotImplementedError


def extract_variable(code_str, name):
    """
    Get the value assigned to a variable with name "name" by passing a code
    string
    """
    variable_found = False
    value = None

    p = parso.parse(code_str)

    for ch in p.children:
        # FIXME: this works but we should find out what's the difference
        # between these two and if they are the only two valid cases
        if ch.type in ['simple_stmt', 'expr_stmt']:
            if ch.type == 'simple_stmt':
                stmt = ch.children[0]
            elif ch.type == 'expr_stmt':
                stmt = ch

            if hasattr(stmt, 'get_defined_names'):
                defined = stmt.get_defined_names()

                if len(defined) == 1 and defined[0].value == name:
                    variable_found = True
                    value = eval(stmt.children[2].get_code())

    return variable_found, value


def extract_upstream_assign(cell_code):
    """
    Infer dependencies from a single Python cell. Looks for a cell that
    defines an upstream variable which must be either a dictionary or None
    """
    upstream_found, upstream = extract_variable(cell_code, 'upstream')

    if not upstream_found:
        raise ValueError("Could not parse a valid 'upstream' variable "
                         "from code:\n'%s'. If the notebook "
                         "does not have dependencies add "
                         "upstream = None" % cell_code)
    else:
        valid_types = (Mapping, list, tuple, set)
        if not (isinstance(upstream, valid_types) or upstream is None):
            raise ValueError("Found an upstream variable but it is not a "
                             "valid type (dictionary, list, tuple set or None "
                             ", got '%s' type from code:\n"
                             "'%s'" % (type(upstream), cell_code))
        elif isinstance(upstream, valid_types):
            return set(upstream)
        else:
            return None
