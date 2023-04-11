from ploomber.sources import SQLScriptSource
from ploomber.tasks._params import Params


def test_script_source():
    source = SQLScriptSource(
        """
{% set product =  PostgresRelation(["schema", "name", "table"]) %}

CREATE TABLE {{product}} AS
SELECT * FROM some_table
"""
    )
    params = Params._from_dict({"product": "this should be ignored"})
    assert source.render(params)
