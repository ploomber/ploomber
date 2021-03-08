{% set product = SQLiteRelation(['transformed', 'table']) %}

CREATE TABLE {{product}} AS
SELECT customer_id, SUM(value) AS value_per_customer
FROM {{upstream['filter']}}
GROUP BY customer_id