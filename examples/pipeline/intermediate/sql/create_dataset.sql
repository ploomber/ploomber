BEGIN;

DROP TABLE IF EXISTS {{product}};

CREATE TABLE {{product}} AS
SELECT
    "fixed acidity",
    density,
    "pH",
    alcohol / "pH" AS alcohol_over_ph,
    CASE WHEN color = 'white' THEN 0 ELSE 1 END AS label
FROM {{upstream['wine']}}
ORDER BY RANDOM();

 ALTER TABLE {{product}} ADD COLUMN id SERIAL PRIMARY KEY;

COMMIT;