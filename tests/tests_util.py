"""
This functions are imported in some tests
"""
import ast
from pathlib import Path

from ploomber.executors import Serial
from itertools import product


def expand_grid(grid):
    tuples = product(*[to_tuple(k, values) for k, values in grid.items()])
    params = [{t[0]: t[1] for t in tuple_} for tuple_ in tuples]
    return params


def to_tuple(k, values):
    return [(k, v) for v in values]


# only these configurations log errors
grid = {
    'build_in_subprocess': [True, False],
    'catch_exceptions': [True],
    'catch_warnings': [True, False]
}

executors_w_exception_logging = [
    Serial(**kwargs) for kwargs in expand_grid(grid)
]


def write_simple_pipeline(file_name, modules, function_name):
    source = f"{'.'.join(modules)}.{function_name}"
    Path(file_name).write_text(f"""
tasks:
    - source: {source}
      product: out.ipynb
""")


def assert_function_in_module(function_name, module_file):
    code = module_file.read_text()
    module = ast.parse(code)
    names = {
        element.name
        for element in module.body if hasattr(element, 'name')
    }
    assert function_name in names
