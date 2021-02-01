from pathlib import Path

import pytest
import numpy as np
import pandas as pd
from jinja2 import Template

from ploomber.clients import SQLAlchemyClient
from ploomber import testing


def test_can_check_nulls(tmp_directory):
    client = SQLAlchemyClient('sqlite:///' + str(Path(tmp_directory, 'db.db')))

    df = pd.DataFrame({'no_nas': [1, 2, 1], 'nas': [1, np.nan, 1]})
    df.to_sql('my_table', client.engine)

    assert not testing.sql.nulls_in_columns(client, ['no_nas'], 'my_table')
    assert testing.sql.nulls_in_columns(client, ['nas'], 'my_table')
    assert testing.sql.nulls_in_columns(client, ['no_nas', 'nas'], 'my_table')


def test_can_check_distinct(tmp_directory):
    client = SQLAlchemyClient('sqlite:///' + str(Path(tmp_directory, 'db.db')))

    df = pd.DataFrame({'no_nas': [1, 2, 1], 'nas': [1, np.nan, 1]})
    df.to_sql('my_table', client.engine)

    assert (testing.sql.distinct_values_in_column(client, 'no_nas',
                                                  'my_table') == {1, 2})

    assert (testing.sql.distinct_values_in_column(client, 'nas',
                                                  'my_table') == {1.0, None})


def test_can_check_duplicates(tmp_directory):
    client = SQLAlchemyClient('sqlite:///' + str(Path(tmp_directory, 'db.db')))

    df = pd.DataFrame({'duplicates': [1, 1], 'no_duplicates': [1, 2]})
    df.to_sql('my_table', client.engine)

    assert not testing.sql.duplicates_in_column(client, 'no_duplicates',
                                                'my_table')
    assert testing.sql.duplicates_in_column(client, 'duplicates', 'my_table')

    # check cols that have duplicates but do not have duplicate pairs

    df = pd.DataFrame({'a': [1, 1, 1], 'b': [1, 2, 3]})
    df.to_sql('another_table', client.engine)
    assert not testing.sql.duplicates_in_column(client, ['a', 'b'],
                                                'another_table')


@pytest.mark.parametrize('stats', [False, True])
def test_assert_no_duplicates_in_column(tmp_directory, stats):
    client = SQLAlchemyClient('sqlite:///' + str(Path(tmp_directory, 'db.db')))

    df = pd.DataFrame({'a': [1, 1], 'b': [1, 2]})
    df.to_sql('my_table', client.engine)

    with pytest.raises(AssertionError):
        testing.sql.assert_no_duplicates_in_column(client,
                                                   'a',
                                                   'my_table',
                                                   stats=stats)


def test_duplicates_stats(tmp_directory):
    client = SQLAlchemyClient('sqlite:///' + str(Path(tmp_directory, 'db.db')))

    df = pd.DataFrame({'a': [1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1]})
    df.to_sql('my_table', client.engine)

    n_rows, n_unique, n_duplicates = (testing.sql.duplicates_stats(
        client, 'a', 'my_table'))

    assert n_rows == 11
    assert n_unique == 5
    assert n_duplicates == 6


def test_can_check_range(tmp_directory):
    client = SQLAlchemyClient('sqlite:///' + str(Path(tmp_directory, 'db.db')))

    df = pd.DataFrame({'x': [1, 2, 3, 4, 5, 1000]})
    df.to_sql('my_table', client.engine)

    assert testing.sql.range_in_column(client, 'x', 'my_table') == (1, 1000)


def test_exists_row_where(tmp_directory):
    client = SQLAlchemyClient('sqlite:///' + str(Path(tmp_directory, 'db.db')))

    df = pd.DataFrame({'x': [1, 2, 3, 4]})
    df.to_sql('my_table', client.engine)

    assert testing.sql.exists_row_where(client, 'x > 1', 'my_table')
    assert not testing.sql.exists_row_where(client, 'x > 4', 'my_table')


sql_t = Template("""
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
""")


@pytest.mark.parametrize('trailing', [False, True])
@pytest.mark.parametrize('parenthesis', [False, True])
def test_sql_parser(trailing, parenthesis):

    sql = sql_t.render(trailing=trailing, parenthesis=parenthesis)

    m = testing.sql.SQLParser(sql)

    assert list(m) == ['a', 'b', '_select']
    assert m['a'] == 'select * from aa'
    assert m['b'] == 'select * from bb'
    assert m['_select'] == 'select * from a join b on col\n'

    code_a = m.until('a')
    code_b = m.until('b')

    assert code_a == ('\nWITH a AS (\n    select * from aa\n)\n'
                      'SELECT * FROM a LIMIT 20')
    assert code_b == (
        '\nWITH a AS (\n    select * from aa\n), b '
        'AS (\n    select * from bb\n)\nSELECT * FROM b LIMIT 20')


@pytest.mark.parametrize('trailing', [False, True])
def test_sql_parser_custom_select(trailing):

    sql = sql_t.render(trailing=trailing)

    m = testing.sql.SQLParser(sql)

    code_a = m.until('a', select='SELECT * FROM a WHERE x < 10')
    code_b = m.until('b', select='SELECT * FROM b WHERE x < 10')

    assert code_a == ('\nWITH a AS (\n    select * from aa\n)\nSELECT * '
                      'FROM a WHERE x < 10')
    assert code_b == (
        '\nWITH a AS (\n    select * from aa\n), b '
        'AS (\n    select * from bb\n)\nSELECT * FROM b WHERE x < 10')


@pytest.mark.parametrize('trailing', [False, True])
def test_sql_parser_add_clause(trailing):
    sql = sql_t.render(trailing=trailing)
    m = testing.sql.SQLParser(sql)
    m['c'] = 'select * from cc'

    assert m.until(
        'c', limit=None) == ('\nWITH a AS (\n    select * from aa\n), b AS '
                             '(\n    select * from bb\n), c AS (\n    '
                             'select * from cc\n)\nSELECT * FROM c')


def test_sql_parser_insert():
    sql = sql_t.render(trailing=False)
    m = testing.sql.SQLParser(sql)

    m2 = m.insert('zero', 'select * from zero', inplace=False)
    assert list(m2) == ['zero', 'a', 'b', '_select']
    assert list(m) == ['a', 'b', '_select']

    m.insert('zero', 'select * from zero', inplace=True)
    assert list(m) == ['zero', 'a', 'b', '_select']
    assert m.until(
        'a',
        limit=None) == ('\nWITH zero AS (\n    select * from zero\n),'
                        ' a AS (\n    select * from aa\n)\nSELECT * FROM a')
