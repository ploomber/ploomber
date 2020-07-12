DROP TABLE IF EXISTS my_schema.agg;

CREATE TABLE my_schema.agg AS
SELECT user_id, kind, COUNT(*)
FROM my_schema.users
JOIN my_schema.actions
USING  (user_id)
GROUP BY user_id, kind
