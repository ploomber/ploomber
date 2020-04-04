from ploomber.Table import Row, Table


def test_row():
    r = Row({'a': 1, 'b': 2})
    assert (str(r) == '  a    b\n---  ---\n  1    2'
            # python 3.5 does not guarantee order
            or str(r) == '  b    a\n---  ---\n  2    1')


def test_table():
    r = Row({'a': 1, 'b': 2})
    t = Table([r, r])
    assert (str(t) == '  a    b\n---  ---\n  1    2\n  1    2'
            # python 3.5 does not guarantee order
            or str(t) == '  b    a\n---  ---\n  2    1\n  2    1')
