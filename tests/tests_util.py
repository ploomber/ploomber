from ploomber.executors import Serial
from itertools import product


def expand_grid(grid):
    tuples = product(*[to_tuple(k, values) for k, values in grid.items()])
    params = [{t[0]: t[1] for t in tuple_} for tuple_ in tuples]
    return params


def to_tuple(k, values):
    return [(k, v) for v in values]


# only these configurations log errors
grid = {'build_in_subprocess': [True, False],
        'catch_exceptions': [True],
        'catch_warnings': [True, False]}

executors_w_exception_logging = [Serial(**kwargs) for kwargs in
                                 expand_grid(grid)]
