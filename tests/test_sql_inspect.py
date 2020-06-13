from ploomber.sql.inspect import extract_upstream


def test_extract_upstream():
    sql = """CREATE TABLE {{product}} something AS
SELECT * FROM {{upstream['some_task']}}
JOIN {{upstream['another_task']}}
USING some_column
"""
    assert extract_upstream(sql) == {'some_task', 'another_task'}
