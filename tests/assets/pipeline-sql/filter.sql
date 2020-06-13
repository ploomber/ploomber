SELECT * FROM {{upstream['load']}}
WHERE purchase_date > TO_DATE('2015-01-01', 'YYYY-MM-DD')