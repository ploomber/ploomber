{% set product = 'filtered' %}

CREATE TABLE {{product}} AS
SELECT * FROM {{upstream['load.sql']}}
WHERE strftime(purchase_date) > date('2015-01-01')