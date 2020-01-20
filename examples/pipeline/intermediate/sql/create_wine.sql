BEGIN;

DROP TABLE IF EXISTS {{product}};

CREATE TABLE {{product}} AS
SELECT *, 'red' AS color FROM {{upstream['red']}}
UNION ALL
SELECT *, 'white' AS color FROM {{upstream['white']}}

COMMIT;