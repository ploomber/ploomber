DROP TABLE IF EXISTS {{product}};

CREATE TABLE {{product}} AS
SELECT * FROM users
WHERE signup_date >= '2018-01-01';
