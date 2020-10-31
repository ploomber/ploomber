from pathlib import Path
from unittest.mock import Mock

import pytest

from ploomber.exceptions import SourceInitializationError
from ploomber.sources import (SQLQuerySource, SQLScriptSource, GenericSource,
                              PythonCallableSource, FileSource, Source,
                              NotebookSource)
from ploomber.tasks import SQLScript
from ploomber.products import SQLiteRelation, GenericSQLRelation
from ploomber import DAG
from ploomber._testing_utils import assert_no_extra_attributes_in_class

from test_pkg import functions


@pytest.mark.parametrize('concrete_class', Source.__subclasses__())
def test_interface(concrete_class):
    """
    Look for unnecessary implemeneted methods/attributes in MetaProduct,
    this helps us keep the API up-to-date if the Product interface changes
    """
    if concrete_class in {PythonCallableSource, NotebookSource}:
        pytest.xfail()

    allowed_mapping = {}

    allowed = allowed_mapping.get(concrete_class.__name__, {})

    assert_no_extra_attributes_in_class(Source,
                                        concrete_class,
                                        allowed=allowed)


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


@pytest.mark.parametrize('class_', [SQLScriptSource, SQLQuerySource])
def test_sql_repr_unrendered(class_):
    name = class_.__name__

    # short case
    s = class_('SELECT * FROM {{product}}')
    assert repr(s) == name + '(SELECT * FROM {{product}})'

    # long case, adds ...
    s = class_(("SELECT * FROM {{product}} WHERE "
                "some_very_very_long_column "
                "= 'some_very_large_value'"))
    assert repr(s) == name + ("(SELECT * FROM {{product}} "
                              "WHERE some_very_very_long_column "
                              "= 'some_very_large...)")

    # multi line case, adds ...
    s = class_(("SELECT * FROM {{product}}\nWHERE "
                "some_very_very_long_column"
                " = 'some_very_large_value'"))
    assert repr(s) == name + '(SELECT * FROM {{product}}...)'


@pytest.mark.parametrize('class_', [SQLScriptSource, SQLQuerySource])
def test_sql_repr_rendered(class_):
    name = class_.__name__
    render_arg = {'product': GenericSQLRelation(('schema', 'name', 'table'))}
    s = class_('SELECT * FROM {{product}}')
    s.render(render_arg)

    assert repr(s) == name + '(SELECT * FROM schema.name)'

    s = class_(("SELECT * FROM {{product}} WHERE "
                "some_very_very_long_column "
                "= 'some_very_large_value'"))
    s.render(render_arg)
    assert repr(s) == name + ("(SELECT * FROM schema.name "
                              "WHERE some_very_very_long_column "
                              "= 'some_very_large...)")

    s = class_(("SELECT * FROM {{product}}\nWHERE "
                "some_very_very_long_column"
                " = 'some_very_large_value'"))
    s.render(render_arg)
    assert repr(s) == name + '(SELECT * FROM schema.name...)'


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


def test_warns_if_sql_scipt_does_not_create_relation(
        sqlite_client_and_tmp_dir):
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


def test_warns_if_number_of_relations_does_not_match_products(
        sqlite_client_and_tmp_dir):
    client, _ = sqlite_client_and_tmp_dir
    dag = DAG()
    dag.clients[SQLiteRelation] = client

    sql = """
    -- wrong sql, products must be used in CREATE statements
    CREATE TABLE {{product[0]}} AS
    SELECT * FROM my_table
    """

    t = SQLScript(sql, [
        SQLiteRelation((None, 'my_table', 'table')),
        SQLiteRelation((None, 'another_table', 'table'))
    ],
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


# TODO: use fixture and backup the entire test_pkg source code
# TODO: check all other relevant properties are updated as well
def test_hot_reload(path_to_test_pkg):
    path_to_functions = Path(path_to_test_pkg, 'functions.py')
    source = PythonCallableSource(functions.some_function, hot_reload=True)

    source_old = path_to_functions.read_text()
    source_new = 'def some_function():\n    1 + 1\n'
    path_to_functions.write_text(source_new)

    assert str(source) == source_new

    path_to_functions.write_text(source_old)


@pytest.mark.parametrize('class_', [SQLScriptSource, SQLQuerySource])
def test_hot_reload_sql_sources(class_, tmp_directory):
    path = Path(tmp_directory, 'script.sql')
    path.write_text('/*doc*/\n{{product}}')

    product = SQLiteRelation(('some_table', 'table'))

    source = class_(path, hot_reload=True)
    source.render({'product': product})

    assert str(source) == '/*doc*/\nsome_table'
    assert source.variables == {'product'}
    assert source.doc == 'doc'

    path.write_text('/*new doc*/\n{{product}} {{new_tag}}')
    source.render({'product': product, 'new_tag': 'modified'})

    assert str(source) == '/*new doc*/\nsome_table modified'
    assert source.variables == {'product', 'new_tag'}
    assert source.doc == 'new doc'


def test_hot_reload_generic_source(tmp_directory):
    path = Path(tmp_directory, 'script.sql')
    path.write_text('/*doc*/\n{{product}}')

    source = GenericSource(path, hot_reload=True)
    source.render({'product': 'some_table'})

    assert str(source) == '/*doc*/\nsome_table'
    assert source.variables == {'product'}

    path.write_text('/*new doc*/\n{{product}} {{new_tag}}')
    source.render({'product': 'some_table', 'new_tag': 'modified'})

    assert str(source) == '/*new doc*/\nsome_table modified'
    assert source.variables == {'product', 'new_tag'}


def test_python_callable_properties(path_to_test_pkg):
    source = PythonCallableSource(functions.simple_w_docstring)

    file_, line = source.loc.split(':')

    assert source.doc == functions.simple_w_docstring.__doc__
    assert source.name == 'simple_w_docstring'
    assert file_ == functions.__file__
    assert line == '21'
    assert PythonCallableSource.__name__ in repr(source)
    assert functions.simple_w_docstring.__name__ in repr(source)
    assert source.loc in repr(source)


def test_python_callable_with_dotted_path_does_not_import(
        tmp_directory, add_current_to_sys_path):
    Path('some_dotted_path.py').write_text("""
import some_unknown_package

def some_fn():
    pass
""")
    # doing it this way should not import anything
    source = PythonCallableSource('some_dotted_path.some_fn')

    assert str(source) == 'def some_fn():\n    pass\n'
    assert source.name == 'some_fn'


def test_python_callable_with_dotted_path(tmp_directory,
                                          add_current_to_sys_path):
    Path('some_other_dotted_path.py').write_text("""
def some_fn():
    pass
""")
    # doing it this way should not import anything
    source = PythonCallableSource('some_other_dotted_path.some_fn')
    assert callable(source.primitive)


def test_dotted_path_and_callable_give_same_source():
    a = PythonCallableSource(functions.simple_w_docstring)
    b = PythonCallableSource('test_pkg.functions.simple_w_docstring')

    assert str(a) == str(b)


@pytest.mark.parametrize(
    'class_', [SQLScriptSource, SQLQuerySource, GenericSource, FileSource])
def test_file_location_included_if_initialized_from_file(
        class_, tmp_directory):
    path = Path('source.txt')
    path.write_text("""
    CREATE TABLE {{product}} AS SELECT * FROM X
    """)
    source = class_(path)
    assert 'source.txt' in repr(source)


@pytest.mark.parametrize('source, expected', [
    ['{{upstream["key"]}}', {'key'}],
    [
        '{{upstream["key"]}} - {{upstream["another_key"]}}',
        {'key', 'another_key'}
    ],
    ['{{hello}}', None],
])
def test_extract_upstream_from_file_source(source, expected):
    assert FileSource(source).extract_upstream() == expected
