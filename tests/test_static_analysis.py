import pytest
from ploomber.static_analysis import notebook, sql, project

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


@pytest.mark.parametrize('code, expected',
                         [cell_case_1, cell_case_2])
def test_infer_from_code_cell(code, expected):
    assert (sorted(notebook.infer_dependencies_from_code_cell(code))
            == sorted(expected))


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


@pytest.mark.parametrize('code, expected',
                         [nb_case_1, nb_case_2])
def test_infer_from_code_str(code, expected):
    assert (sorted(notebook.infer_dependencies_from_code_str(code,
                                                             fmt='py'))
            == sorted(expected))


def test_extract_upstream_sql():
    code = """CREATE TABLE {{product}} something AS
SELECT * FROM {{upstream['some_task']}}
JOIN {{upstream['another_task']}}
USING some_column
"""
    assert sql.extract_upstream_from_sql(code) == {'some_task', 'another_task'}


@pytest.mark.parametrize('templates', [None,
                                       ['filter.sql',
                                        'load.sql', 'transform.sql']])
def test_infer_dependencies_from_path(path_to_tests, templates):
    path = path_to_tests / 'assets' / 'pipeline-sql'
    expected = {'filter.sql': {'load.sql'}, 'transform.sql': {'filter.sql'}}
    assert project.infer_depencies_from_path(path,
                                             templates=templates) == expected
