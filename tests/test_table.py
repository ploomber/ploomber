import string
import shutil
from textwrap import TextWrapper
from collections import namedtuple
from unittest.mock import Mock

import pytest
import pandas as pd
from ploomber.table import (
    Row,
    Table,
    BuildReport,
    rows2columns,
    wrap_elementwise,
    calculate_wrapping,
    width_required_for_column,
    apply_wrapping,
    equal_column_width,
    separator_width,
)

from ploomber import table


@pytest.mark.parametrize(
    "header_length, max_value_length, expected",
    [
        [1, 1, 3],
        [1, 2, 3],
        [1, 3, 3],
        [1, 4, 4],
        [1, 5, 5],
        [2, 1, 4],
        [3, 1, 5],
    ],
)
def test_separator_width(header_length, max_value_length, expected):
    assert separator_width(header_length, max_value_length) == expected


@pytest.mark.parametrize(
    "header, values, expected",
    [
        ["short", ["row", "some large row"], len("some large row")],
        ["some long header", ["row", "row"], len("some long header") + 2],
        ["", [], 2],
        ["some header", [], len("some header") + 2],
    ],
    ids=[
        "uses-largest-row",
        "accounts-for-header-spacing",
        "no-header-no-rows",
        "no-rows",
    ],
)
def test_width_required_for_column(header, values, expected):
    assert width_required_for_column(header, values) == expected


@pytest.mark.parametrize(
    "space, sizes, excluded, width_expected",
    [
        # 100 = 32 * (3 columns) + 2 * 2 (between-column spacing)
        [100, [2, 3, 5], [], 32],
        # 15 = (2 + 3) (two short columns) + 5 + 2 * 2 (between-column spacing)
        [15, [2, 3, 10], ["a", "b"], 5],
        # Same as above but let the algorithm figure out that it can exclude
        # a and b to maximize space for the longer column
        [15, [2, 3, 10], [], 5],
        # when thre is nothing to wrap returns space available
        [99, [2, 3, 10], ["a", "b", "c"], 99],
    ],
)
def test_calculate_wrapping(space, sizes, excluded, width_expected):
    table_dict = {k: [k * s] for k, s in zip(string.ascii_letters, sizes)}
    assert calculate_wrapping(table_dict, excluded, space) == width_expected


@pytest.mark.parametrize(
    "value, wrapped",
    [
        ["1234", "123\n4"],
        [["1234", "1234"], ["123\n4", "123\n4"]],
    ],
)
def test_wrap_elementwise(value, wrapped):
    wrapper = TextWrapper(width=3)
    assert wrap_elementwise(value, wrapper) == wrapped


@pytest.mark.parametrize(
    "table_dict, wrapped, exclude",
    [
        [{"a": "1234"}, {"a": "123\n4"}, None],
        [{"a": ["1234"], "b": ["1234"]}, {"a": ["123\n4"], "b": ["1234"]}, ["b"]],
        # check header wrapping accounts for the extra space needed
        [
            {
                "header": ["1234"],
            },
            {
                "h\ne\na\nd\ne\nr": ["123\n4"],
            },
            None,
        ],
    ],
)
def test_apply_wrapping(table_dict, wrapped, exclude):
    wrapper = TextWrapper(width=3)
    assert apply_wrapping(table_dict, wrapper, exclude) == wrapped


def test_rows2columns():
    r1 = Row({"a": 1})
    r2 = Row({"a": 2})

    assert rows2columns([r1, r2]) == {"a": [1, 2]}


def test_row_str_and_repr():
    r = Row({"a": 1, "b": 2})
    expected = "  a    b\n---  ---\n  1    2"

    assert str(r) in expected
    assert repr(r) in expected
    # parse html representation with pandas
    html = pd.read_html(r._repr_html_())[0]
    assert html.to_dict() == {"a": {0: 1}, "b": {0: 2}}


def test_row_str_setitem():
    r = Row({"a": 1, "b": 2})
    r["a"] = 10
    assert r["a"] == 10


def test_table_str_and_repr(monkeypatch):
    mock = Mock()
    mock.get_terminal_size().columns = 6
    monkeypatch.setattr(table, "shutil", mock)

    r = Row({"a": 1, "b": 2})
    t = Table([r, r])
    expected = "  a    b\n---  ---\n  1    2\n  1    2"

    assert str(t) == expected
    assert repr(t) == expected
    # parse html representation with pandas
    html = pd.read_html(t._repr_html_())[0]
    assert html.to_dict(orient="list") == {"a": [1, 1], "b": [2, 2]}


def test_table_iter():
    r = Row({"a": 1, "b": 2})
    t = Table([r, r])
    assert set(iter(t)) == {"a", "b"}


def test_table_wrap():
    r = Row({"a": "abc d", "b": "abc d"})
    table = Table([r, r], column_width=3)
    # Max expected length: 3 (col a) + 2 (whitespace) + 3 (col b) = 8
    assert max([len(line) for line in str(table).splitlines()]) == 8


def test_table_auto_size(monkeypatch):
    TerminalSize = namedtuple("TerminalSize", ["columns"])
    monkeypatch.setattr(shutil, "get_terminal_size", lambda: TerminalSize(80))

    r = Row({"a": "1" * 60, "b": "1" * 60})
    table = Table([r, r], column_width="auto")

    assert max([len(line) for line in str(table).splitlines()]) == 80

    # simulate resize
    monkeypatch.setattr(shutil, "get_terminal_size", lambda: TerminalSize(120))
    assert max([len(line) for line in str(table).splitlines()]) == 120


def test_select_multiple_cols_in_row():
    r = Row({"a": 1, "b": 2})
    assert r[["a", "b"]] == {"a": 1, "b": 2}


def test_error_if_row_initialized_with_non_mapping():
    with pytest.raises(TypeError):
        Row([])


def test_empty_table():
    return Table({})


def test_select_col_in_table():
    r = Row({"a": 1, "b": 2})
    t = Table([r, r], column_width=None)
    assert t["a"] == [1, 1]


def test_select_multiple_cols_in_table():
    d = {"a": 1, "b": 2}
    r = Row(d)
    t = Table([r, r], column_width=None)
    assert t[["a", "b"]] == {"a": [1, 1], "b": [2, 2]}


def test_table_values():
    d = {"a": 1, "b": 2}
    r = Row(d)
    t = Table([r, r], column_width=None)
    assert t.values == {"a": [1, 1], "b": [2, 2]}


def test_create_build_report():
    row = Row({"Elapsed (s)": 1})
    report = BuildReport([row, row])
    assert report == {"Elapsed (s)": [1, 1], "Percentage": [50, 50]}


def test_convert_to_pandas():
    d = {"a": 1, "b": 2}
    r = Row(d)
    t = Table([r, r], column_width=None)
    expected = pd.DataFrame({"a": [1, 1], "b": [2, 2]})
    assert expected.equals(t.to_pandas())


def test_convert_to_dict():
    d = {"a": 1, "b": 2}
    r = Row(d)
    t = Table([r, r], column_width=None)
    assert t.to_dict() == {"a": [1, 1], "b": [2, 2]}


@pytest.mark.parametrize(
    "n_cols, width_total, expected",
    [
        [1, 10, 10],
        # 10 (total) = 2 (n_cols) * 4 (width) + 2 (between column spacing)
        [2, 10, 4],
        # degenerate case: there isn't enough space to display, return 1
        [2, 1, 1],
    ],
)
def test_equal_column_width(n_cols, width_total, expected):
    assert equal_column_width(n_cols, width_total) == expected


@pytest.mark.parametrize("width_total", [1, 2, 3])
def test_warns_if_not_enough_space(width_total):
    with pytest.warns(UserWarning) as record:
        equal_column_width(n_cols=2, width_total=width_total)

    assert len(record) == 1
    expected = (
        "Not enough space to display 2 columns with a width "
        f"of {width_total}. Using a column width of 1"
    )
    assert record[0].message.args[0] == expected


def test_complete_keys():
    a = dict(i=1, k=2)
    b = dict(z=3)

    table = Table.from_dicts([a, b], complete_keys=True)

    assert table.to_dict() == {"k": [2, ""], "z": ["", 3], "i": [1, ""]}
