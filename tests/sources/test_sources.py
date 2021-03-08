from pathlib import Path
from unittest.mock import Mock

import pytest

from ploomber.exceptions import SourceInitializationError
from ploomber.sources import (SQLQuerySource, SQLScriptSource, GenericSource,
                              PythonCallableSource, FileSource, NotebookSource)
from ploomber.sources.abc import Source
from ploomber.tasks import SQLScript
from ploomber.products import SQLiteRelation, GenericSQLRelation
from ploomber import DAG
from ploomber._testing_utils import assert_no_extra_attributes_in_class

from test_pkg import functions


@pytest.mark.parametrize('concrete_class', Source.__subclasses__())
def test_interface(concrete_class):
    """
    Check that Source concrete classes do not have any extra methods
    that are not declared in the Source abstract class
    """
    if concrete_class in {PythonCallableSource, NotebookSource}:
        # FIXME: these two have a lot of extra methods
        pytest.xfail()

    allowed_mapping = {}

    allowed = allowed_mapping.get(concrete_class.__name__, {})

    assert_no_extra_attributes_in_class(Source,
                                        concrete_class,
                                        allowed=allowed)


@pytest.mark.parametrize('class_, arg, expected', [
    [PythonCallableSource, functions.simple, 'simple'],
    [NotebookSource, Path('source.py'), 'source'],
    [SQLQuerySource, Path('query.sql'), 'query'],
    [SQLScriptSource, Path('script.sql'), 'script'],
    [GenericSource, Path('script.sh'), 'script'],
])
def test_name(tmp_directory, class_, arg, expected):
    Path('source.py').write_text("""
# + tags=["parameters"]
# -
""")

    Path('query.sql').touch()
    Path('script.sql').write_text(
        'CREATE TABLE {{product}} AS SELECT * FROM data')
    Path('script.sh').touch()
    Path('file.ext').touch()

    assert class_(arg).name == expected


def test_generic_source_unrendered():
    s = GenericSource('some {{placeholder}}')
    assert repr(s) == "GenericSource('some {{placeholder}}')"

    s = GenericSource(('some {{placeholder}} and some other large text '
                       'with important details that we should not include'))
    assert repr(s) == "GenericSource('some {{placeholde...should not include')"


def test_generic_source_rendered():
    s = GenericSource('some {{placeholder}}')
    s.render({'placeholder': 'placeholder'})
    assert repr(s) == "GenericSource('some placeholder')"

    s = GenericSource(('some {{placeholder}} and some other large text '
                       'with important details that we should not include'))
    s.render({'placeholder': 'placeholder'})
    assert repr(s) == "GenericSource('some placeholder ...should not include')"


@pytest.mark.parametrize('class_', [SQLScriptSource, SQLQuerySource])
def test_sql_repr_unrendered(class_):
    name = class_.__name__

    # short case
    s = class_('SELECT * FROM {{product}}')
    assert repr(s) == name + "('SELECT * FROM {{product}}')"

    # long case, adds ...
    s = class_(("SELECT * FROM {{product}} WHERE "
                "some_very_very_long_column "
                "= 'some_very_large_value'"))
    assert repr(s) == name + "(\"SELECT * FROM {{p..._very_large_value'\")"


@pytest.mark.parametrize('class_', [SQLScriptSource, SQLQuerySource])
def test_sql_repr_rendered(class_):
    name = class_.__name__
    render_arg = {'product': GenericSQLRelation(('schema', 'name', 'table'))}
    s = class_('SELECT * FROM {{product}}')
    s.render(render_arg)

    assert repr(s) == name + "('SELECT * FROM schema.name')"

    s = class_(("SELECT * FROM {{product}} WHERE "
                "some_very_very_long_column "
                "= 'some_very_large_value'"))
    s.render(render_arg)
    assert repr(s) == name + ("(\"SELECT * FROM sch..._very_large_value'\")")


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
    with pytest.raises(SourceInitializationError) as excinfo:
        SQLScriptSource('SELECT * FROM my_table')

    expected = (
        "Error initializing SQLScriptSource('SELECT * FROM my_table'): "
        "The {{product}} placeholder is required. "
        "Example: 'CREATE TABLE {{product}} AS (SELECT * FROM ...)'")
    assert expected == str(excinfo.value)


def test_warns_if_sql_script_does_not_create_relation(
        sqlite_client_and_tmp_dir):
    client, _ = sqlite_client_and_tmp_dir
    dag = DAG()
    dag.clients[SQLiteRelation] = client

    mock_client = Mock()
    mock_client.split_source = ';'

    t = SQLScript('SELECT * FROM {{product}}',
                  SQLiteRelation((None, 'my_table', 'table')),
                  dag=dag,
                  client=mock_client,
                  name='sql')

    with pytest.warns(UserWarning) as record:
        t.render()

    assert len(record) == 1
    msg = ('It appears that your script will not create any tables/views but '
           "the product parameter is "
           "SQLiteRelation(schema=None, name='my_table', kind='table')")
    assert record[0].message.args[0] == msg


def test_warns_if_number_of_relations_does_not_match_number_of_products():
    product = [
        SQLiteRelation((None, 'my_table', 'table')),
        SQLiteRelation((None, 'another_table', 'table'))
    ]

    source = SQLScriptSource("""
    CREATE TABLE {{product[0]}} AS
    SELECT * FROM my_table
    GROUP BY some_column
    """)

    with pytest.warns(UserWarning) as record:
        source.render({'product': product})

    assert len(record) == 1
    msg = ('It appears that your script will create 1 relation(s) '
           'but you declared 2 product(s): '
           "[SQLiteRelation(schema=None, name='my_table', kind='table'), "
           "SQLiteRelation(schema=None, name='another_table', kind='table')]")
    assert record[0].message.args[0] == msg


def test_warns_if_no_create_statement_found():
    product = SQLiteRelation((None, 'my_table', 'table'))

    source = SQLScriptSource("""
    -- {{product}} without CREATE statement
    SELECT * FROM my_table
    GROUP BY some_column
    """)

    with pytest.warns(UserWarning) as record:
        source.render({'product': product})

    assert len(record) == 1
    msg = ('It appears that your script will not create any tables/views '
           'but the product parameter is '
           "SQLiteRelation(schema=None, name='my_table', kind='table')")
    assert record[0].message.args[0] == msg


@pytest.mark.parametrize(
    'sql, split_source',
    [
        [
            """DROP TABLE IF EXISTS {{product}};
        CREATE TABLE {{product}} AS SELECT * FROM another""", None
        ],
        [
            """
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE {{product}}';
    EXCEPTION
        WHEN OTHERS THEN NULL;
    END;

    ##

    CREATE TABLE {{product}} AS
    SELECT * FROM my_table
    GROUP BY some_column
    """, '##'
        ],
    ],
)
def test_doesnt_warn_if_relations_match(sql, split_source):
    product = SQLiteRelation((None, 'my_table', 'table'))

    source = SQLScriptSource(sql, split_source=split_source)

    with pytest.warns(None) as record:
        source.render({'product': product})

    assert not record


def test_warns_if_inferred_relations_do_not_match_product():
    product = SQLiteRelation((None, 'my_table', 'table'))

    source = SQLScriptSource("""
    -- using {{product}} in the wrong place
    CREATE TABLE some_table AS SELECT * FROM another_table
    """)

    with pytest.warns(UserWarning) as record:
        source.render({'product': product})

    assert len(record) == 1
    msg = ('It appears that your script will create relations '
           '{ParsedSQLRelation(schema=None, '
           'name=\'some_table\', kind=\'table\')}, which doesn\'t match '
           'products: {SQLiteRelation(schema=None, '
           'name=\'my_table\', kind=\'table\')}. Make sure schema, '
           'name and kind (table or view) match')
    assert record[0].message.args[0] == msg


# TODO: check all other relevant properties are updated as well
def test_hot_reload(backup_test_pkg):
    path_to_functions = Path(backup_test_pkg, 'functions.py')
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

    tokens = source.loc.split(':')
    file_, line = ':'.join(tokens[:-1]), tokens[-1]

    assert source.doc == functions.simple_w_docstring.__doc__
    assert source.name == 'simple_w_docstring'
    assert file_ == functions.__file__
    assert line == '23'
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


def test_error_if_python_callable_does_not_need_product_but_has_it():
    def fn(product):
        pass

    with pytest.raises(TypeError) as excinfo:
        PythonCallableSource(fn, needs_product=False).render({})

    assert ("Function 'fn' should not have a 'product' "
            "parameter, but return its result instead") == str(excinfo.value)


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
