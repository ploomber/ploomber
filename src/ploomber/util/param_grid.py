from ploomber.io import pretty_print

from collections.abc import Mapping, Iterable
from itertools import product, chain


class Interval:
    """Generate intervals from lower to upper of size delta, last interval
    will be smaller if needed

    >>> from ploomber.util import Interval
    >>> from datetime import date
    >>> from dateutil.relativedelta import relativedelta
    >>> interval = Interval(date(year=2020, month=1, day=1),
    ...                     date(year=2022, month=6, day=1),
    ...                     relativedelta(years=1)).expand()
    >>> interval[0]
    (datetime.date(2020, 1, 1), datetime.date(2021, 1, 1))
    >>> interval[1]
    (datetime.date(2021, 1, 1), datetime.date(2022, 1, 1))
    >>> interval[2]
    (datetime.date(2022, 1, 1), datetime.date(2022, 6, 1))
    """

    def __init__(self, lower, upper, delta):
        if lower >= upper:
            raise ValueError("lower must be strictly lower than upper")

        self.lower = lower
        self.upper = upper
        self.delta = delta

    def expand(self):
        tuples = []
        cursor = self.lower
        i = 0

        while True:
            cursor = self.lower + (i + 1) * self.delta

            if cursor < self.upper:
                tuples.append((self.lower + i * self.delta, cursor))
                i += 1
            else:
                tuples.append((self.lower + i * self.delta, self.upper))
                break

        return tuples

    def __repr__(self):
        return "Interval from {} to {} with delta {}".format(
            self.lower, self.upper, self.delta
        )


class ParamGrid:
    """Generate parameter grids

    Parameters
    ----------
    grid : list or dict
        Grid to generate. Can generate multiple grids at once by passing
        a list of grids

    params : dict
        Parameters that should remain fixed

    Examples
    --------
    >>> pg = ParamGrid({'a': [1, 2, 3], 'b': [2, 4, 6]})
    >>> list(pg.zip())
    [{'a': 1, 'b': 2}, {'a': 2, 'b': 4}, {'a': 3, 'b': 6}]

    >>> list(pg.product())  # doctest: +NORMALIZE_WHITESPACE
    [{'a': 1, 'b': 2}, {'a': 1, 'b': 4}, {'a': 1, 'b': 6}, {'a': 2, 'b': 2},
    {'a': 2, 'b': 4}, {'a': 2, 'b': 6}, {'a': 3, 'b': 2}, {'a': 3, 'b': 4},
    {'a': 3, 'b': 6}]

    >>> pg = ParamGrid({'a': Interval(0, 10, 2), 'b': [2, 4, 6, 8, 10]})
    >>> list(pg.zip())  # doctest: +NORMALIZE_WHITESPACE
    [{'a': (0, 2), 'b': 2}, {'a': (2, 4), 'b': 4}, {'a': (4, 6), 'b': 6},
    {'a': (6, 8), 'b': 8}, {'a': (8, 10), 'b': 10}]

    Notes
    -----
    Parameters with a single element are converted to lists of length 1
    """

    def __init__(self, grid, params=None):
        if isinstance(grid, Mapping):
            grid = [grid]

        self._expanded = [_expand(d) for d in grid]
        self._params = params or dict()

    def zip(self):
        for d in chain(self._expanded):
            lengths = set(len(v) for v in d.values())

            if len(lengths) != 1:
                raise ValueError("All parameters should have the same length")

            length = list(lengths)[0]

            for i in range(length):
                out = {k: v[i] for k, v in d.items()}
                _check_keys_overlap(out, self._params)
                yield {**out, **self._params}

    def product(self):
        for d in chain(self._expanded):
            keys = d.keys()
            values = d.values()

            for elements in product(*values):
                d = {}

                for k, v in zip(keys, elements):
                    d[k] = v

                _check_keys_overlap(d, self._params)

                yield {**d, **self._params}


def _expand(d):
    expanded = {}

    for k, v in d.items():
        if isinstance(v, Interval):
            expanded[k] = v.expand()
        elif not isinstance(v, Iterable) or isinstance(v, str):
            expanded[k] = [v]
        else:
            expanded[k] = v

    return expanded


def _check_keys_overlap(a, b):
    overlap = set(a) & set(b)

    if overlap:
        overlap_ = pretty_print.iterable(overlap)
        raise ValueError(
            "Error generating grid: 'grid' and 'params' "
            f"have overlapping keys: {overlap_}"
        )
