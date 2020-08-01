from ploomber.sources import SQLScriptSource
from ploomber.tasks.Params import Params


def test_script_source():
    source = SQLScriptSource("""
{% set product =  PostgresRelation(["schema", "name", "table"]) %}

CREATE TABLE {{product}} AS
SELECT * FROM some_table
""")
    params = Params()
    params._dict['product'] = 'this should be ignored'
    assert source.render(params)
