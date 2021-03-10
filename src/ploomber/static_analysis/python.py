"""
Extract "upstream" and "product" from Python notebooks
"""
import ast
import parso
from collections.abc import Mapping

from ploomber.static_analysis.abc import NotebookExtractor, Extractor


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
                             f"from code: {self.parameters_cell!r}")
        else:
            return product


def get_key_value(node):
    return node.value.id if hasattr(node.value, 'id') else None


def get_constant(node):
    # python 3.9 node.slice.value for earlier versions
    if isinstance(node.slice, ast.Constant):
        return node.slice
    # for earlier versions (if the attribute doesn't exist we are dealing with
    # the upstream[some_name]) case
    elif hasattr(node.slice, 'value'):
        return node.slice.value


def get_value(node):
    constant = get_constant(node)
    # .s for python 3.7 and 3.7, .value for 3.8 and newer
    return constant.s if hasattr(constant, 's') else constant.value


class PythonCallableExtractor(Extractor):
    def extract_upstream(self):
        """
        Extract keys requested to an upstream variable (e.g. upstream['key'])
        """
        module = ast.parse(self.code)
        return {
            get_value(node)
            for node in ast.walk(module)
            if isinstance(node, ast.Subscript) and get_key_value(node) ==
            'upstream' and isinstance(get_constant(node), ast.Str)
        } or None

    def extract_product(self):
        raise NotImplementedError('Extract product is not implemented '
                                  'for python callables')


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
                         "in the 'parameters' cell with code:\n'%s'\n"
                         "If the notebook does not have dependencies add "
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
