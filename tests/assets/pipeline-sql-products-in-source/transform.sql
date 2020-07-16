{% set product = SQLiteRelation(['transformed', 'table']) %}

CREATE TABLE {{product}} AS
SELECT customer_id, SUM(value) AS value_per_customer
FROM {{upstream['filter.sql']}}
GROUP BY customer_id