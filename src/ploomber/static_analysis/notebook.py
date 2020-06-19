from collections.abc import Mapping
import parso


def infer_dependencies_from_code_str(code_str):
    upstream = None

    p = parso.parse(code_str)

    for ch in p.children:
        if ch.type == 'simple_stmt':
            stmt = ch.children[0]
            defined = stmt.get_defined_names()

            if len(defined) == 1 and defined[0].value == 'upstream':
                upstream = eval(stmt.children[2].get_code())

    if not isinstance(upstream, Mapping):
        raise ValueError("Could not parse a valid dictionary called "
                         "'upstream' from code:\n'%s'" % code_str)

    return list(upstream.keys())
