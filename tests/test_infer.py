import pytest

from ploomber.sql import infer
from ploomber.sql.infer import ParsedSQLRelation
from ploomber.products import (GenericSQLRelation, SQLRelation, SQLiteRelation,
                               PostgresRelation)


def test_unquoted_relation_to_str():
    assert str(ParsedSQLRelation(None, 'data', 'table')) == 'data'


def test_unquoted_relation_w_schema_to_str():
    assert str(ParsedSQLRelation('schema', 'data', 'table')) == 'schema.data'


def test_quoted_relation_to_str():
    assert str(ParsedSQLRelation(None, '"data"', 'table')) == '"data"'


def test_quoted_relation_w_schema_to_str():
    assert (str(ParsedSQLRelation('"schema"', '"data"',
                                  'table')) == '"schema"."data"')


def test_detects_create_table_w_schema():
    ct = infer.created_relations("CREATE TABLE my_schema.my_table")
    assert len(ct) == 1 and ct[0] == infer.ParsedSQLRelation(
        'my_schema', 'my_table', 'table')


def test_detects_create_table_wo_schema():
    ct = infer.created_relations("CREATE TABLE my_table")
    assert len(ct) == 1 and ct[0] == infer.ParsedSQLRelation(
        None, 'my_table', 'table')


sql_long = """
CREATE VIEW schema.name  AS (
    WITH step_a AS (
        SELECT * FROM a
    ), step_b AS (
        SELECT * FROM B
    )

    SELECT * FROM step_a JOIN step_b USING (column)
"""


@pytest.mark.parametrize(
    'sql, schema, name, kind',
    [
        [
            'CREATE TABLE schema.name2 AS ( SELECT * FROM a )',
            'schema',
            'name2',
            'table',
        ],
        [
            'CREATE TABLE a.b AS SELECT * FROM a ',
            'a',
            'b',
            'table',
        ],
        [
            'CREATE TABLE a.b AS SELECT * FROM a; DROP VIEW some_view',
            'a',
            'b',
            'table',
        ],
        [
            'DROP VIEW b; CREATE VIEW b AS SELECT * FROM a ',
            None,
            'b',
            'view',
        ],
        [
            sql_long,
            'schema',
            'name',
            'view',
        ],
    ],
)
def test_parsing_create_statement(sql, schema, name, kind):
    relations = infer.created_relations(sql)

    assert len(relations) == 1
    assert relations[0] == ParsedSQLRelation(schema=schema,
                                             name=name,
                                             kind=kind)


def test_does_not_break_if_passing_invalid_sql():
    sql = 'THIS ISNT VALID SQL'
    relations = infer.created_relations(sql)

    assert not relations


def test_parses_create_table():
    rels = infer.created_relations('CREATE TABLE x; SELECT * FROM y')
    assert rels[0] == ParsedSQLRelation(schema=None, name='x', kind='table')


def test_parses_create_table_w_quotes():
    rels = infer.created_relations('CREATE TABLE "x"')
    assert rels[0] == ParsedSQLRelation(schema=None, name='x', kind='table')


@pytest.mark.parametrize('sql', [
    'DROP TABLE x; CREATE TABLE x',
    'DROP TABLE IF EXISTS x; CREATE TABLE x',
])
def test_drop_then_create(sql):
    rels = infer.created_relations(sql)
    assert rels[0] == ParsedSQLRelation(schema=None, name='x', kind='table')


def test_create_then_drop():
    rels = infer.created_relations('CREATE TABLE x; DROP TABLE x;')
    assert not rels


def test_parses_create_view():
    rels = infer.created_relations('create view x; SELECT * FROM y')
    assert rels[0] == ParsedSQLRelation(schema=None, name='x', kind='view')


def test_repr():
    rel = ParsedSQLRelation(schema=None, name='name', kind='view')

    assert repr(rel) == "ParsedSQLRelation(('name', 'view'))"


@pytest.mark.parametrize('class_', [
    GenericSQLRelation,
    SQLRelation,
    SQLiteRelation,
    PostgresRelation,
])
def test_equality_sql_products(class_):

    r = ParsedSQLRelation(schema='schema', name='name', kind='table')
    r1 = class_(['schema', 'name', 'table'])

    assert r == r1
    assert hash(r) == hash(r1)
