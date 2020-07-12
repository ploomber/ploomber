DROP TABLE IF EXISTS {{product}};

CREATE TABLE {{product}} AS
SELECT * FROM actions
WHERE kind IN ('action_a', 'action_b');
