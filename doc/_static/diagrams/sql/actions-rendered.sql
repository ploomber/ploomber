DROP TABLE IF EXISTS my_schema.actions;

CREATE TABLE my_schema.actions AS
SELECT * FROM actions
WHERE kind IN ('action_a', 'action_b');
