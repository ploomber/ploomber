DROP TABLE IF EXISTS my_schema.users;

CREATE TABLE my_schema.users AS
SELECT * FROM users
WHERE signup_date >= '2018-01-01';
