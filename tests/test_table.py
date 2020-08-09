import pandas as pd
from ploomber.Table import Row, Table, BuildReport


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


def test_select_multiple_cols_in_row():
    r = Row({'a': 1, 'b': 2})
    assert r[['a', 'b']] == {'a': 1, 'b': 2}


def test_empty_table():
    return Table({})


def test_select_col_in_table():
    r = Row({'a': 1, 'b': 2})
    t = Table([r, r])
    assert t['a'] == [1, 1]


def test_select_multiple_cols_in_table():
    d = {'a': 1, 'b': 2}
    r = Row(d)
    t = Table([r, r])
    assert t[['a', 'b']] == [d, d]


def test_table_values():
    d = {'a': 1, 'b': 2}
    r = Row(d)
    t = Table([r, r])
    assert t.values == {'a': [1, 1], 'b': [2, 2]}


def test_create_build_report():
    row = Row({'Elapsed (s)': 1})
    report = BuildReport([row, row])
    row_out = {'Elapsed (s)': 1, 'Percentage': 50}
    assert report == [row_out, row_out]


def test_convert_to_pandas():
    d = {'a': 1, 'b': 2}
    r = Row(d)
    t = Table([r, r])
    expected = pd.DataFrame({'a': [1, 1], 'b': [2, 2]})
    assert expected.equals(t.to_pandas())


def test_convert_to_dict():
    d = {'a': 1, 'b': 2}
    r = Row(d)
    t = Table([r, r])
    assert t.to_dict() == {'a': [1, 1], 'b': [2, 2]}
