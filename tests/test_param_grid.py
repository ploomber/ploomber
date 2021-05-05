import datetime
from dateutil.relativedelta import relativedelta
from ploomber.util import ParamGrid, Interval


def compare(a, b):
    for element in a:
        if element not in b:
            return False

    return len(a) == len(b)


def test_interval():
    interval = Interval(datetime.date(year=2010, month=1, day=1),
                        datetime.date(year=2012, month=1, day=1),
                        relativedelta(years=1))
    expanded = interval.expand()
    repr_ = ('Interval from 2010-01-01 to 2012-01-01 with '
             'delta relativedelta(years=+1)')
    expected = [(datetime.date(2010, 1, 1), datetime.date(2011, 1, 1)),
                (datetime.date(2011, 1, 1), datetime.date(2012, 1, 1))]

    assert expanded == expected
    assert repr(interval) == repr_


def test_param_grid():
    pg = ParamGrid({'a': [1, 2, 3], 'b': [2, 4, 6]})
    assert compare(list(pg.zip()), [{
        'a': 1,
        'b': 2
    }, {
        'a': 2,
        'b': 4
    }, {
        'a': 3,
        'b': 6
    }])
    assert compare(list(pg.product()), [{
        'a': 1,
        'b': 2
    }, {
        'a': 1,
        'b': 4
    }, {
        'a': 1,
        'b': 6
    }, {
        'a': 2,
        'b': 2
    }, {
        'a': 2,
        'b': 4
    }, {
        'a': 2,
        'b': 6
    }, {
        'a': 3,
        'b': 2
    }, {
        'a': 3,
        'b': 4
    }, {
        'a': 3,
        'b': 6
    }])


def test_param_grid_w_interval():
    pg = ParamGrid({'a': Interval(0, 10, 2), 'b': [2, 4, 6, 8, 10]})
    assert compare(list(pg.zip()), [{
        'a': (0, 2),
        'b': 2
    }, {
        'a': (2, 4),
        'b': 4
    }, {
        'a': (4, 6),
        'b': 6
    }, {
        'a': (6, 8),
        'b': 8
    }, {
        'a': (8, 10),
        'b': 10
    }])


def test_param_grid_list():
    first = {'a': [1, 2], 'b': [1, 2]}
    second = {'c': [3, 4], 'd': [3, 4]}
    pg = ParamGrid([first, second])

    assert list(pg.product()) == [{
        'a': 1,
        'b': 1
    }, {
        'a': 1,
        'b': 2
    }, {
        'a': 2,
        'b': 1
    }, {
        'a': 2,
        'b': 2
    }, {
        'c': 3,
        'd': 3
    }, {
        'c': 3,
        'd': 4
    }, {
        'c': 4,
        'd': 3
    }, {
        'c': 4,
        'd': 4
    }]
