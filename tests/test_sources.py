import pytest
from unittest.mock import Mock

from ploomber.exceptions import SourceInitializationError
from ploomber.sources import SQLQuerySource, SQLScriptSource, GenericSource
from ploomber.tasks import SQLScript
from ploomber.products import SQLiteRelation
from ploomber import DAG
from ploomber.products import GenericSQLRelation
from ploomber.sql import infer


def test_generic_source_unrendered():
    s = GenericSource('some {{placeholder}}')
    assert repr(s) == 'GenericSource(some {{placeholder}})'

    s = GenericSource(('some {{placeholder}} and some other large text '
                       'with important details that we should not include'))
    assert repr(s) == ('GenericSource(some {{placeholder}} and some other '
                       'large text with important details that we...)')

    s = GenericSource('some {{placeholder}}\nand some other large text ')
    assert repr(s) == 'GenericSource(some {{placeholder}}...)'


def test_generic_source_rendered():
    s = GenericSource('some {{placeholder}}')
    s.render({'placeholder': 'placeholder'})
    assert repr(s) == 'GenericSource(some placeholder)'

    s = GenericSource(('some {{placeholder}} and some other large text '
                       'with important details that we should not include'))
    s.render({'placeholder': 'placeholder'})
    assert repr(s) == ('GenericSource(some placeholder and some other '
                       'large text with important details that we sho...)')

    s = GenericSource('some {{placeholder}}\nand some other large text ')
    s.render({'placeholder': 'placeholder'})
    assert repr(s) == 'GenericSource(some placeholder...)'


@pytest.mark.parametrize('class_',
                         [SQLScriptSource, SQLQuerySource])
def test_sql_repr_unrendered(class_):
    name = class_.__name__

    # short case
    s = class_('SELECT * FROM {{product}}')
    assert repr(s) == name+'(SELECT * FROM {{product}})'

    # long case, adds ...
    s = class_(("SELECT * FROM {{product}} WHERE "
                "some_very_very_long_column "
                "= 'some_very_large_value'"))
    assert repr(s) == name+("(SELECT * FROM {{product}} "
                            "WHERE some_very_very_long_column "
                            "= 'some_very_large...)")

    # multi line case, adds ...
    s = class_(("SELECT * FROM {{product}}\nWHERE "
                "some_very_very_long_column"
                " = 'some_very_large_value'"))
    assert repr(s) == name+'(SELECT * FROM {{product}}...)'


@pytest.mark.parametrize('class_',
                         [SQLScriptSource, SQLQuerySource])
def test_sql_repr_rendered(class_):
    name = class_.__name__
    render_arg = {'product': GenericSQLRelation(('schema', 'name', 'table'))}
    s = class_('SELECT * FROM {{product}}')
    s.render(render_arg)

    assert repr(s) == name+'(SELECT * FROM schema.name)'

    s = class_(("SELECT * FROM {{product}} WHERE "
                "some_very_very_long_column "
                "= 'some_very_large_value'"))
    s.render(render_arg)
    assert repr(s) == name+("(SELECT * FROM schema.name "
                            "WHERE some_very_very_long_column "
                            "= 'some_very_large...)")

    s = class_(("SELECT * FROM {{product}}\nWHERE "
                "some_very_very_long_column"
                " = 'some_very_large_value'"))
    s.render(render_arg)
    assert repr(s) == name+'(SELECT * FROM schema.name...)'


def test_can_parse_sql_docstring():
    source = SQLQuerySource('/* docstring */ SELECT * FROM customers')
    assert source.doc == ' docstring '


def test_can_parse_sql_docstring_from_unrendered_template():
    source = SQLQuerySource(
        '/* get data from {{table}} */ SELECT * FROM {{table}}')
    assert source.doc == ' get data from {{table}} '


def test_can_parse_sql_docstring_from_rendered_template():
    source = SQLQuerySource(
        '/* get data from {{table}} */ SELECT * FROM {{table}}')
    source.render({'table': 'my_table'})
    assert source.doc == ' get data from my_table '


def test_cannot_initialize_sql_script_with_literals():
    with pytest.raises(SourceInitializationError):
        SQLScriptSource('SELECT * FROM my_table')


def test_warns_if_sql_scipt_does_not_create_relation(sqlite_client_and_tmp_dir):
    client, _ = sqlite_client_and_tmp_dir
    dag = DAG()
    dag.clients[SQLiteRelation] = client

    t = SQLScript('SELECT * FROM {{product}}',
                  SQLiteRelation((None, 'my_table', 'table')),
                  dag=dag,
                  client=Mock(),
                  name='sql')

    match = 'will not create any tables or views but the task has product'

    with pytest.warns(UserWarning, match=match):
        t.render()


def test_warns_if_number_of_relations_does_not_match_products(sqlite_client_and_tmp_dir):
    client, _ = sqlite_client_and_tmp_dir
    dag = DAG()
    dag.clients[SQLiteRelation] = client

    sql = """
    -- wrong sql, products must be used in CREATE statements
    CREATE TABLE {{product[0]}} AS
    SELECT * FROM my_table
    """

    t = SQLScript(sql,
                  [SQLiteRelation((None, 'my_table', 'table')),
                   SQLiteRelation((None, 'another_table', 'table'))],
                  dag=dag,
                  client=Mock(),
                  name='sql')

    match = r'.*will create 1 relation\(s\) but you declared 2 product\(s\).*'

    with pytest.warns(UserWarning, match=match):
        t.render()


# def test_warns_if_name_does_not_match(dag):
#     dag = DAG()
#     p = PostgresRelation(('schema', 'name', 'table'))
#     t = SQLScript("""CREATE TABLE schema.name2 AS (SELECT * FROM a);
#                           -- {{product}}
#                           """, p,
#                   dag, 't', client=Mock())
#     t.render()


# templates

# def test_warns_if_no_product_found_using_template(fake_conn):
#     dag = DAG()

#     p = PostgresRelation(('schema', 'sales', 'table'))

#     with pytest.warns(UserWarning):
#         SQLScript(Template("SELECT * FROM {{name}}"), p, dag, 't',
#                           params=dict(name='customers'))

# comparing metaproduct
