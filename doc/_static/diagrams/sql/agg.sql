DROP TABLE IF EXISTS {{product}};

CREATE TABLE {{product}} AS
SELECT user_id, kind, COUNT(*)
FROM {{upstream['users']}}
JOIN {{upstream['actions']}}
USING  (user_id)
GROUP BY user_id, kind
