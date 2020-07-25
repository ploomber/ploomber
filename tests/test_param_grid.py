import datetime
from dateutil.relativedelta import relativedelta
from ploomber.util import ParamGrid, Interval


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
    assert sorted(list(pg.zip())) == sorted([{
        'a': 1,
        'b': 2
    }, {
        'a': 2,
        'b': 4
    }, {
        'a': 3,
        'b': 6
    }])
    assert sorted(list(pg.product())) == sorted([{
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
    assert sorted(list(pg.zip())) == sorted([{
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
