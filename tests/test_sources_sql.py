# from jinja2 import Template
# from jinja2 import Environment
# from ploomber.sources import SQLScriptSource

# source = SQLScriptSource("""
# {% set product =  PostgresRelation(["schema", "name", "table"]) %}

# CREATE TABLE {{product}} AS
# SELECT * FROM some_table
# """)
# source._placeholder._raw
# source.render({product.node.name: class_})
