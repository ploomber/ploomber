"""
Utilities for extracting docstrings
"""
import re


def extract_from_sql(code):
    """Extract from a SQL script"""
    # [start] [any charaters] [\] [*] [any word chars] [*] [/] [any word chars]
    regex = r"^\s*\/\*([\w\W]+)\*\/[\w\W]*"
    match = re.match(regex, code)
    return "" if match is None else match.group(1)


def extract_from_nb(nb):
    """Extract from a notebook object"""
    if not nb.cells:
        return ""

    first = nb.cells[0]

    if first.cell_type == "markdown":
        return first.source
    elif first.cell_type == "code":
        return extract_from_triple_quotes(first.source)
    else:
        return ""


def extract_from_triple_quotes(code):
    """Extract from code string defined as triple single/double quotes"""
    return extract_from_triple_single_quotes(code) or extract_from_triple_double_quotes(
        code
    )


def extract_from_triple_single_quotes(code):
    regex = r"^\s*'''([\w\W]+)'''[\w\W]*"
    match = re.match(regex, code)
    return "" if match is None else match.group(1)


def extract_from_triple_double_quotes(code):
    regex = r'^\s*"""([\w\W]+)"""[\w\W]*'
    match = re.match(regex, code)
    return "" if match is None else match.group(1)
