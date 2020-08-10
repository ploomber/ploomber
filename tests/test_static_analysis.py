import pytest
from ploomber.static_analysis.python import PythonNotebookExtractor
from ploomber.static_analysis.sql import SQLExtractor
from ploomber.static_analysis.string import StringExtractor
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
    extractor = PythonNotebookExtractor(parameters_cell=code)
    inferred = extractor.extract_upstream()
    assert sorted(inferred) == sorted(expected)


case_error_1 = "some_variable = 1"
case_error_2 = """
upstreammm = {'a': 1}
"""
case_error_3 = "upstream = 1"


@pytest.mark.parametrize('code', [case_error_1, case_error_2, case_error_3])
def test_error_from_code_cell(code):
    extractor = PythonNotebookExtractor(parameters_cell=code)

    with pytest.raises(ValueError):
        extractor.extract_upstream()


def test_extract_upstream_sql():
    code = """CREATE TABLE {{product}} something AS
SELECT * FROM {{upstream['some_task']}}
JOIN {{upstream['another_task']}}
USING some_column
"""
    extractor = SQLExtractor(code)
    assert extractor.extract_upstream() == {'some_task', 'another_task'}


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
    extracted = SQLExtractor(code).extract_product()
    assert isinstance(extracted, class_)
    assert extracted.name == name
    assert extracted.schema == schema
    assert extracted.kind == kind


def test_extract_product_from_sql_none_case():
    code = 'some code that does not define a product variable'
    assert SQLExtractor(code).extract_product() is None


@pytest.mark.parametrize('source, expected', [
    ['{{upstream["key"]}}', {'key'}],
    [
        '{{upstream["key"]}} - {{upstream["another_key"]}}',
        {'key', 'another_key'}
    ],
    ['{{hello}}', None],
])
def test_string_extractor(source, expected):
    assert StringExtractor(source).extract_upstream() == expected
