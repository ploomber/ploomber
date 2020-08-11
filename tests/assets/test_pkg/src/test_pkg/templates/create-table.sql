{% import "macros.sql" as m %}
DROP TABLE IF EXISTS {{product}};
CREATE TABLE {{product}} AS
SELECT * FROM {{m.my_macro()}}