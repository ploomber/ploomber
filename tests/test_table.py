from pathlib import Path
import string
import shutil
from textwrap import TextWrapper
from collections import namedtuple

import pytest
import pandas as pd
from ploomber.Table import (Row, Table, BuildReport, rows2columns, wrap_value,
                            wrap_mapping, auto_determine_column_width)


@pytest.mark.parametrize(
    'space, sizes, excluded, excluded_expected, width_expected', [
        [100, [2, 3, 5], [], ['a', 'b', 'c'], 0],
        [15, [2, 3, 10], ['a'], ['a', 'b'], 6],
    ])
def test_auto_determine_column_width(space, sizes, excluded, excluded_expected,
                                     width_expected):
    values = {k: [k * s] for k, s in zip(string.ascii_letters, sizes)}

    excluded_out, width_out = auto_determine_column_width(
        values, excluded, space)

    assert sorted(excluded_out) == sorted(excluded_expected)
    assert width_out == width_expected


@pytest.mark.parametrize('value, wrapped', [
    ['1234', '123\n4'],
    [['1234', '1234'], ['123\n4', '123\n4']],
])
def test_wrap_value(value, wrapped):
    wrapper = TextWrapper(width=3)
    assert wrap_value(value, wrapper) == wrapped


@pytest.mark.parametrize('mapping, wrapped, exclude', [
    [{
        'a': '1234'
    }, {
        'a': '123\n4'
    }, None],
    [{
        'a': ['1234'],
        'b': ['1234']
    }, {
        'a': ['123\n4'],
        'b': ['1234']
    }, ['b']],
])
def test_wrap_mapping(mapping, wrapped, exclude):
    wrapper = TextWrapper(width=3)
    assert wrap_mapping(mapping, wrapper, exclude) == wrapped


def test_rows2columns():
    r1 = Row({'a': 1})
    r2 = Row({'a': 2})

    assert rows2columns([r1, r2]) == {'a': [1, 2]}


def test_row_str_and_repr():
    r = Row({'a': 1, 'b': 2})
    # we need both because python 3.5 does not guarantee order
    expected = {
        '  a    b\n---  ---\n  1    2',
        '  b    a\n---  ---\n  2    1',
    }

    assert str(r) in expected
    assert repr(r) in expected
    # parse html representation with pandas
    html = pd.read_html(r._repr_html_())[0]
    assert html.to_dict() == {'a': {0: 1}, 'b': {0: 2}}


def test_row_str_setitem():
    r = Row({'a': 1, 'b': 2})
    r['a'] = 10
    assert r['a'] == 10


def test_table_str_and_repr():
    r = Row({'a': 1, 'b': 2})
    t = Table([r, r])
    # we need both because python 3.5 does not guarantee order
    expected = {
        '  a    b\n---  ---\n  1    2\n  1    2',
        '  b    a\n---  ---\n  2    1\n  2    1',
    }

    assert str(t) in expected
    assert repr(t) in expected
    # parse html representation with pandas
    html = pd.read_html(t._repr_html_())[0]
    assert html.to_dict(orient='list') == {'a': [1, 1], 'b': [2, 2]}


def test_table_iter():
    r = Row({'a': 1, 'b': 2})
    t = Table([r, r])
    assert set(iter(t)) == {'a', 'b'}


def test_table_wrap():
    r = Row({'a': 'abc d', 'b': 'abc d'})
    table = Table([r, r], column_width=3)
    # Max expected length: 3 (col a) + 2 (whitespace) + 3 (col b) = 8
    assert max([len(line) for line in str(table).splitlines()]) == 8


def test_table_auto_size(monkeypatch):
    TerminalSize = namedtuple('TerminalSize', ['columns'])
    monkeypatch.setattr(shutil, 'get_terminal_size', lambda: TerminalSize(80))

    r = Row({'a': '1' * 60, 'b': '1' * 60})
    table = Table([r, r], column_width='auto')

    assert max([len(line) for line in str(table).splitlines()]) == 80

    # simulate resize
    monkeypatch.setattr(shutil, 'get_terminal_size', lambda: TerminalSize(120))
    assert max([len(line) for line in str(table).splitlines()]) == 120


def test_select_multiple_cols_in_row():
    r = Row({'a': 1, 'b': 2})
    assert r[['a', 'b']] == {'a': 1, 'b': 2}


def test_error_if_row_initialized_with_non_mapping():
    with pytest.raises(TypeError):
        Row([])


def test_empty_table():
    return Table({})


def test_select_col_in_table():
    r = Row({'a': 1, 'b': 2})
    t = Table([r, r], column_width=None)
    assert t['a'] == [1, 1]


def test_select_multiple_cols_in_table():
    d = {'a': 1, 'b': 2}
    r = Row(d)
    t = Table([r, r], column_width=None)
    assert t[['a', 'b']] == {'a': [1, 1], 'b': [2, 2]}


def test_table_values():
    d = {'a': 1, 'b': 2}
    r = Row(d)
    t = Table([r, r], column_width=None)
    assert t.values == {'a': [1, 1], 'b': [2, 2]}


def test_create_build_report():
    row = Row({'Elapsed (s)': 1})
    report = BuildReport([row, row])
    assert report == {'Elapsed (s)': [1, 1], 'Percentage': [50, 50]}


def test_convert_to_pandas():
    d = {'a': 1, 'b': 2}
    r = Row(d)
    t = Table([r, r], column_width=None)
    expected = pd.DataFrame({'a': [1, 1], 'b': [2, 2]})
    assert expected.equals(t.to_pandas())


def test_convert_to_dict():
    d = {'a': 1, 'b': 2}
    r = Row(d)
    t = Table([r, r], column_width=None)
    assert t.to_dict() == {'a': [1, 1], 'b': [2, 2]}
