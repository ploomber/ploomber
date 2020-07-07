import jupytext
from collections.abc import Mapping
import parso

from ploomber.sources.NotebookSource import _find_cell_with_tag


def extract_variable_from_parameters(code_str, fmt, variable):
    """
    Infer upstream dependencies from a .py or .ipynb source string.
    Must contain a "parameters" cell
    """
    nb = jupytext.reads(code_str, fmt)
    cell, _ = _find_cell_with_tag(nb, 'parameters')

    if cell is None:
        raise ValueError('Notebook does not have a cel with the tag '
                         '"parameters"')

    if variable == 'upstream':
        return infer_dependencies_from_code_cell(cell.source)
    elif variable == 'product':
        return get_product(cell.source)
    else:
        raise ValueError('variable must be upstream or product')


def extract_variable_from_code_cell(code_str, name):
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

            defined = stmt.get_defined_names()

            if len(defined) == 1 and defined[0].value == name:
                variable_found = True
                value = eval(stmt.children[2].get_code())

    return variable_found, value


def get_product(code_str):
    product_found, product = extract_variable_from_code_cell(code_str,
                                                             'product')

    if not product_found:
        raise ValueError("Could not parse a valid 'product' variable "
                         "from code:\n%s" % code_str)
    else:
        return product


def infer_dependencies_from_code_cell(code_str):
    """
    Infer dependencies from a single Python cell. Looks for a cell that
    defines an upstream variable which must be either a dictionary or None
    """
    upstream_found, upstream = extract_variable_from_code_cell(code_str,
                                                               'upstream')

    if not upstream_found:
        raise ValueError("Could not parse a valid 'upstream' variable "
                         "from code:\n'%s'. If the notebook "
                         "does not have dependencies add "
                         "upstream = None" % code_str)
    else:
        valid_types = (Mapping, list, tuple, set)
        if not (isinstance(upstream, valid_types) or upstream is None):
            raise ValueError("Found an upstream variable but it is not a "
                             "valid type (dictionary, list, tuple set or None "
                             ", got '%s' type from code:\n"
                             "'%s'" % (type(upstream), code_str))
        elif isinstance(upstream, valid_types):
            return set(upstream)
        else:
            return None
