import jupytext
from collections.abc import Mapping
import parso

from ploomber.sources.NotebookSource import _find_cell_with_tag


def infer_dependencies_from_code_str(code_str, fmt):
    """
    Infer upstream dependencies from a .py or .ipynb source string.
    Must contain a "parameters" cell
    """
    nb = jupytext.reads(code_str, fmt)
    cell, _ = _find_cell_with_tag(nb, 'parameters')

    if cell is None:
        raise ValueError('Notebook does not have a cel with the tag '
                         '"parameters"')

    return infer_dependencies_from_code_cell(cell.source)


def infer_dependencies_from_code_cell(code_str):
    """Infer dependencies from a single Python cell
    """
    upstream = None

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

            if len(defined) == 1 and defined[0].value == 'upstream':
                upstream = eval(stmt.children[2].get_code())

    if not isinstance(upstream, Mapping):
        raise ValueError("Could not parse a valid dictionary called "
                         "'upstream' from code:\n'%s'" % code_str)

    return list(upstream.keys())
