from ploomber.sql.inspect import extract_upstream, infer_depencies_from_path


def test_extract_upstream():
    sql = """CREATE TABLE {{product}} something AS
SELECT * FROM {{upstream['some_task']}}
JOIN {{upstream['another_task']}}
USING some_column
"""
    assert extract_upstream(sql) == {'some_task', 'another_task'}


def test_infer_dependencies_from_path(path_to_tests):
    path = path_to_tests / 'assets' / 'pipeline-sql'
    expected = {'filter.sql': {'load'}, 'transform.sql': {'filter'}}
    assert infer_depencies_from_path(path) == expected
