from ploomber.Table import Row, Table


def test_row():
    r = Row({'a': 1, 'b': 2})
    assert str(r) == '  a    b\n---  ---\n  1    2'


def test_table():
    r = Row({'a': 1, 'b': 2})
    t = Table([r, r])
    assert str(t) == '  a    b\n---  ---\n  1    2\n  1    2'
