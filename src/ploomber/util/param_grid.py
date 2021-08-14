from collections.abc import Mapping, Iterable
from itertools import product, chain


class Interval:
    """Generate intervals from lower to upper of size delta, last interval
    will be smaller if needed

    >>> from ploomber.util import Interval
    >>> from datetime import date
    >>> from dateutil.relativedelta import relativedelta
    >>> Interval(date(year=2010, month=1, day=1),
    ...          date(year=2019, month=6, day=1),
    ...          relativedelta(years=1)).expand()
    """
    def __init__(self, lower, upper, delta):
        if lower >= upper:
            raise ValueError('lower must be strictly lower than upper')

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
        return ('Interval from {} to {} with delta {}'.format(
            self.lower, self.upper, self.delta))


class ParamGrid:
    """Generate parameter grids

    Parameters
    ----------
    grid : list or dict
        Grid to generate. Can generate multiple grids at once by passing
        a list of grids

    Examples
    --------
    >>> pg = ParamGrid({'a': [1, 2, 3], 'b': [2, 4, 6]})
    >>> list(pg.zip())
    >>> list(pg.product())

    >>> pg = ParamGrid({'a': Interval(0, 10, 2), 'b': [2, 4, 6, 8, 10]})
    >>> list(pg.zip())

    Notes
    -----
    Parameters with a single element are converted to lists of length 1
    """
    def __init__(self, grid):
        if isinstance(grid, Mapping):
            grid = [grid]

        self._expanded = [_expand(d) for d in grid]

    def zip(self):
        for d in chain(self._expanded):
            lengths = set(len(v) for v in d.values())

            if len(lengths) != 1:
                raise ValueError('All parameters should have the same length')

            length = list(lengths)[0]

            for i in range(length):
                yield {k: v[i] for k, v in d.items()}

    def product(self):
        for d in chain(self._expanded):
            keys = d.keys()
            values = d.values()

            for elements in product(*values):
                d = {}

                for k, v in zip(keys, elements):
                    d[k] = v

                yield d


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
