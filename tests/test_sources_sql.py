from ploomber.sources import SQLScriptSource

source = SQLScriptSource("""
{% set product =  PostgresRelation(["schema", "name", "table"]) %}

CREATE TABLE {{product}} AS
SELECT * FROM some_table
""")
x = source.render({'product': 'something'})
