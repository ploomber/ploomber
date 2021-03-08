CREATE TABLE {{product}} AS
SELECT * FROM {{upstream['load']}}
WHERE strftime(purchase_date) > date('2015-01-01')