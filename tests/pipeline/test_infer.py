from ploomber.sql import infer
from ploomber.sql.infer import ParsedSQLRelation


# not able to correctly parse this
# sql = """
# CREATE TABLE schema.name2 AS (
#     SELECT * FROM a
# )
# """

# function breaks with this
# sql = """
# CREATE TABLE schema.name2 AS (
#     CREATE TABLE schema.name2 AS SELECT * FROM a
# )
# """


def test_parses_create_table():
    rels = infer.created_relations('CREATE TABLE x; SELECT * FROM y')
    assert rels[0] == ParsedSQLRelation(schema=None, name='x', kind='table')


def test_parses_create_table_w_quotes():
    rels = infer.created_relations('CREATE TABLE "x"')
    assert rels[0] == ParsedSQLRelation(schema=None, name='x', kind='table')


def test_ignores_drop_create():
    rels = infer.created_relations('DROP TABLE x; CREATE TABLE "x"')
    assert not rels


def test_parses_create_view():
    rels = infer.created_relations('create view x; SELECT * FROM y')
    assert rels[0] == ParsedSQLRelation(schema=None, name='x', kind='view')
