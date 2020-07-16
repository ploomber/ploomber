{% set product = SQLiteRelation(['load', 'table']) %}

CREATE TABLE {{product}} AS
SELECT * FROM sales