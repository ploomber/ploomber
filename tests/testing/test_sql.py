from pathlib import Path

import pytest
import numpy as np
import pandas as pd
from jinja2 import Template

from ploomber.clients import SQLAlchemyClient
from ploomber import testing


def test_can_check_nulls(tmp_directory):
    client = SQLAlchemyClient("sqlite:///" + str(Path(tmp_directory, "db.db")))

    df = pd.DataFrame({"no_nas": [1, 2, 1], "nas": [1, np.nan, 1]})
    df.to_sql("my_table", client.engine)

    assert not testing.sql.nulls_in_columns(client, ["no_nas"], "my_table")
    assert testing.sql.nulls_in_columns(client, ["nas"], "my_table")
    assert testing.sql.nulls_in_columns(client, ["no_nas", "nas"], "my_table")


def test_can_check_distinct(tmp_directory):
    client = SQLAlchemyClient("sqlite:///" + str(Path(tmp_directory, "db.db")))

    df = pd.DataFrame({"no_nas": [1, 2, 1], "nas": [1, np.nan, 1]})
    df.to_sql("my_table", client.engine)

    assert testing.sql.distinct_values_in_column(client, "no_nas", "my_table") == {1, 2}

    assert testing.sql.distinct_values_in_column(client, "nas", "my_table") == {
        1.0,
        None,
    }


def test_can_check_duplicates(tmp_directory):
    client = SQLAlchemyClient("sqlite:///" + str(Path(tmp_directory, "db.db")))

    df = pd.DataFrame({"duplicates": [1, 1], "no_duplicates": [1, 2]})
    df.to_sql("my_table", client.engine)

    assert not testing.sql.duplicates_in_column(client, "no_duplicates", "my_table")
    assert testing.sql.duplicates_in_column(client, "duplicates", "my_table")

    # check cols that have duplicates but do not have duplicate pairs

    df = pd.DataFrame({"a": [1, 1, 1], "b": [1, 2, 3]})
    df.to_sql("another_table", client.engine)
    assert not testing.sql.duplicates_in_column(client, ["a", "b"], "another_table")


@pytest.mark.parametrize("stats", [False, True])
def test_assert_no_duplicates_in_column(tmp_directory, stats):
    client = SQLAlchemyClient("sqlite:///" + str(Path(tmp_directory, "db.db")))

    df = pd.DataFrame({"a": [1, 1], "b": [1, 2]})
    df.to_sql("my_table", client.engine)

    with pytest.raises(AssertionError):
        testing.sql.assert_no_duplicates_in_column(client, "a", "my_table", stats=stats)


def test_duplicates_stats(tmp_directory):
    client = SQLAlchemyClient("sqlite:///" + str(Path(tmp_directory, "db.db")))

    df = pd.DataFrame({"a": [1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1]})
    df.to_sql("my_table", client.engine)

    n_rows, n_unique, n_duplicates = testing.sql.duplicates_stats(
        client, "a", "my_table"
    )

    assert n_rows == 11
    assert n_unique == 5
    assert n_duplicates == 6


def test_can_check_range(tmp_directory):
    client = SQLAlchemyClient("sqlite:///" + str(Path(tmp_directory, "db.db")))

    df = pd.DataFrame({"x": [1, 2, 3, 4, 5, 1000]})
    df.to_sql("my_table", client.engine)

    assert testing.sql.range_in_column(client, "x", "my_table") == (1, 1000)


def test_exists_row_where(tmp_directory):
    client = SQLAlchemyClient("sqlite:///" + str(Path(tmp_directory, "db.db")))

    df = pd.DataFrame({"x": [1, 2, 3, 4]})
    df.to_sql("my_table", client.engine)

    assert testing.sql.exists_row_where(client, "x > 1", "my_table")
    assert not testing.sql.exists_row_where(client, "x > 4", "my_table")


sql_t = Template(
    """
/* some comment */
-- some comment

drop table if exists some_table;

create table some_table as {{'(' if parenthesis else ''}}

/* another comment */

with a as (
    select * from aa
    /*
    multi line
    comment */
), b as (
    -- yet another comment
    select * from bb
)

select * from a join b on col
{{')' if parenthesis else ''}}{{';' if trailing else ''}}
"""
)


@pytest.mark.parametrize("trailing", [False, True])
@pytest.mark.parametrize("parenthesis", [False, True])
def test_sql_parser(trailing, parenthesis):
    sql = sql_t.render(trailing=trailing, parenthesis=parenthesis)

    m = testing.sql.SQLParser(sql)

    assert m._ipython_key_completions_() == ["a", "b", "_select"]
    assert list(m) == ["a", "b", "_select"]
    assert m.mapping["a"] == "select * from aa"
    assert m.mapping["b"] == "select * from bb"
    assert m.mapping["_select"] == "select * from a join b on col\n"

    expected = "\nWITH a AS (\n    select * from aa\n)\n" "SELECT * FROM a LIMIT 20"
    assert m.until("a", parse=False) == expected
    assert str(m["a"]) == expected
    assert m.until("b", parse=False) == (
        "\nWITH a AS (\n    select * from aa\n), b "
        "AS (\n    select * from bb\n)\nSELECT * FROM b LIMIT 20"
    )


sql_t_no_create_statement = """
/* some comment */

drop table if exists some_table;

with a as (
    select * from aa
), b as (
    select * from bb
)
select * from a join b on col
"""


def test_sql_parser_without_create_statement():
    m = testing.sql.SQLParser(sql_t_no_create_statement)

    assert list(m) == ["a", "b", "_select"]
    assert m.mapping["a"] == "select * from aa"
    assert m.mapping["b"] == "select * from bb"
    assert m.mapping["_select"] == "select * from a join b on col\n"


sql_t_no_create_statement_one_subquery = """
WITH a AS (
    select * from aa
)
SELECT * FROM a LIMIT 20
"""


def test_sql_parser_without_create_statement_one_subquery():
    m = testing.sql.SQLParser(sql_t_no_create_statement_one_subquery)

    assert list(m) == ["a", "_select"]
    assert m.mapping["a"] == "select * from aa"
    assert m.mapping["_select"] == "SELECT * FROM a LIMIT 20\n"


sql_one_def_last_select_w_qualifiers = """
WITH a AS (
    select * from aa
)
select a.x, b.y from a join b using (col)
"""

sql_one_def_last_select_w_alias = """
WITH a AS (
    select * from aa
)
select a.x as alias, b.y from a join b using (col)
"""

sql_one_def_last_select_w_alias_space_only = """
WITH a AS (
    select * from aa
)
select a.x alias, b.y from a join b using (col)
"""


@pytest.mark.parametrize(
    "sql, a, _select",
    [
        [
            sql_one_def_last_select_w_qualifiers,
            "select * from aa",
            "select a.x, b.y from a join b using (col)\n",
        ],
        [
            sql_one_def_last_select_w_alias,
            "select * from aa",
            "select a.x as alias, b.y from a join b using (col)\n",
        ],
        [
            sql_one_def_last_select_w_alias_space_only,
            "select * from aa",
            "select a.x alias, b.y from a join b using (col)\n",
        ],
    ],
)
def test_sql_parser_complex_select(sql, a, _select):
    m = testing.sql.SQLParser(sql)

    assert list(m) == ["a", "_select"]
    assert m.mapping["a"] == a
    assert m.mapping["_select"] == _select


@pytest.mark.parametrize("trailing", [False, True])
def test_sql_parser_custom_select(trailing):
    sql = sql_t.render(trailing=trailing)

    m = testing.sql.SQLParser(sql)

    code_a = m.until("a", parse=False, select="SELECT * FROM a WHERE x < 10")
    code_a_w_placeholder = m.until(
        "a", parse=False, select="SELECT * FROM {{key}} WHERE x < 10"
    )
    code_b = m.until("b", parse=False, select="SELECT * FROM b WHERE x < 10")

    expected = "\nWITH a AS (\n    select * from aa\n)\nSELECT * " "FROM a WHERE x < 10"
    assert code_a == expected
    assert code_a_w_placeholder == expected
    assert code_b == (
        "\nWITH a AS (\n    select * from aa\n), b "
        "AS (\n    select * from bb\n)\nSELECT * FROM b WHERE x < 10"
    )


def test_sql_parser_insert_first():
    sql = sql_t.render(trailing=False)
    m = testing.sql.SQLParser(sql)

    m2 = m.insert_first("aa", "select * from aa WHERE x > 10", inplace=False)

    assert m2.mapping["aa"] == "select * from aa WHERE x > 10"
    assert list(m2) == ["aa", "a", "b", "_select"]
    assert list(m) == ["a", "b", "_select"]

    m.insert_first("aa", "select * from aa WHERE x > 10", inplace=True)
    assert list(m) == ["aa", "a", "b", "_select"]
    assert m.until("a", parse=False, limit=None) == (
        "\nWITH aa AS (\n    select * from aa WHERE x > 10\n),"
        " a AS (\n    select * from aa\n)\nSELECT * FROM a"
    )


def test_sql_parser_insert_last():
    sql = sql_t.render(trailing=False)
    m = testing.sql.SQLParser(sql)

    m2 = m.insert_last("select * from last WHERE x > 10", inplace=False)
    assert m2.mapping["_select"] == "select * from last WHERE x > 10"
    assert list(m2) == ["a", "b", "last", "_select"]

    m.insert_last("select * from last WHERE x > 10", inplace=True)
    assert m.mapping["_select"] == "select * from last WHERE x > 10"
    assert list(m) == ["a", "b", "last", "_select"]


def test_sql_parser_replace_last():
    sql = sql_t.render(trailing=False)
    m = testing.sql.SQLParser(sql)

    sub = m.until("a", parse=True)
    sub_ = sub.replace_last("select * from a where x = 1", inplace=False)

    expected = "\nWITH a AS (\n    select * from aa\n)\nselect * from a where x = 1"
    assert str(sub_) == expected
