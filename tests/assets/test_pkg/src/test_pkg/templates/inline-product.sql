{% import "macros.sql" as m %}
{{m.my_comment()}}

{% set product = SQLRelation(['schema', 'name', 'table']) %}

DROP TABLE IF EXISTS {{product}};

CREATE TABLE {{product}} AS
SELECT * FROM {{upstream["some_other_table"]}}