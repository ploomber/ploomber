import pytest
from ploomber.static_analysis import notebook, sql, project
from ploomber.products import (PostgresRelation, SQLiteRelation,
                               GenericSQLRelation, SQLRelation)

cell_case_1 = """
upstream = {'some_key': 'some_value'}
""", ['some_key']

cell_case_2 = """
upstream = {'a': None, 'b': None}
product, some_variable = None, None

if upstream:
    pass

for i in range(10):
    print(i)

if True:
    upstream = {'x': None, 'y': None}
""", ['a', 'b']


@pytest.mark.parametrize('code, expected', [cell_case_1, cell_case_2])
def test_infer_from_code_cell(code, expected):
    assert (sorted(
        notebook.infer_dependencies_from_code_cell(code)) == sorted(expected))


case_error_1 = "some_variable = 1"
case_error_2 = """
upstreammm = {'a': 1}
"""
case_error_3 = "upstream = 1"


@pytest.mark.parametrize('code', [case_error_1, case_error_2, case_error_3])
def test_error_from_code_cell(code):
    with pytest.raises(ValueError):
        notebook.infer_dependencies_from_code_cell(code)


nb_case_1 = """
# + tags=["parameters"]
upstream = {'some_key': 'some_value'}
""", ['some_key']

nb_case_2 = """
# + tags=["parameters"]
upstream = {'a': None, 'b': None}
""", ['a', 'b']


@pytest.mark.parametrize('code, expected', [nb_case_1, nb_case_2])
def test_extract_upstream_from_parameters(code, expected):
    assert (sorted(
        notebook.extract_variable_from_parameters(
            code, fmt='py', variable='upstream')) == sorted(expected))


def test_extract_upstream_sql():
    code = """CREATE TABLE {{product}} something AS
SELECT * FROM {{upstream['some_task']}}
JOIN {{upstream['another_task']}}
USING some_column
"""
    assert sql.extract_upstream_from_sql(code) == {'some_task', 'another_task'}


@pytest.mark.parametrize('code, class_, schema, name, kind', [
    [
        '{% set product =  PostgresRelation(["s", "n", "table"]) %} some code',
        PostgresRelation, 's', 'n', 'table'
    ],
    [
        'simulating some code here {% set product =  SQLiteRelation(["s", "n", "view"]) %}',
        SQLiteRelation, 's', 'n', 'view'
    ],
    [
        '{% set product =  GenericSQLRelation(["s", "n", "table"]) %}',
        GenericSQLRelation, 's', 'n', 'table'
    ],
    [
        '{% set product =  SQLRelation(["s", "n", "view"]) %}', SQLRelation,
        's', 'n', 'view'
    ]
])
def test_extract_product_from_sql(code, class_, schema, name, kind):
    extracted = sql.extract_product_from_sql(code)
    assert isinstance(extracted, class_)
    assert extracted.name == name
    assert extracted.schema == schema
    assert extracted.kind == kind


def test_extract_product_from_sql_none_case():
    code = 'some code that does not define a product variable'
    assert sql.extract_product_from_sql(code) is None


@pytest.mark.parametrize(
    'kwargs', [{
        'templates': ['filter.sql', 'load.sql', 'transform.sql'],
        'match': None
    }])
def test_infer_dependencies_from_path(path_to_tests, kwargs):
    path = path_to_tests / 'assets' / 'pipeline-sql'
    expected = {
        'filter.sql': {'load.sql'},
        'transform.sql': {'filter.sql'},
        'load.sql': set()
    }
    out = project.infer_from_path(path, upstream=True, product=False, **kwargs)
    assert out['upstream'] == expected


def test_extract_variables_from_notebooks(path_to_tests):
    path = path_to_tests / 'assets' / 'nbs'
    expected = {'clean.py': {'load'}, 'plot.py': {'clean'}, 'load.py': set()}
    out = project.infer_from_path(path,
                                  templates=['load.py', 'clean.py', 'plot.py'],
                                  upstream=True,
                                  product=True)
    expected_product = {
        'clean.py': {
            'data': 'output/clean.csv',
            'nb': 'output/clean.ipynb'
        },
        'load.py': {
            'data': 'output/data.csv',
            'nb': 'output/load.ipynb'
        },
        'plot.py': 'output/plot.ipynb'
    }

    assert out['upstream'] == expected
    assert out['product'] == expected_product
