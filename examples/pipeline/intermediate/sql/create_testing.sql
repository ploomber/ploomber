BEGIN;

DROP TABLE IF EXISTS {{product}};

CREATE TABLE {{product}} AS
SELECT
    "fixed acidity",
    density,
    "pH",
    alcohol_over_ph,
    label
FROM {{upstream['dataset']}}
WHERE id > (SELECT 0.7 * COUNT(*) FROM {{upstream['dataset']}});

COMMIT;