CREATE TABLE {{product}} AS
SELECT * FROM {{upstream['load.sql']}}
WHERE purchase_date > '2015-01-01'