/* Add description here
*/

[% if extract_upstream -%]
/* if this task has upstream dependencies, reference them in the SQL query*/
[% else %]
/* if this task has upstream dependencies, declare them the YAML spec */
[% endif %]

[% if extract_product -%]
/* declare a "product" variable in this file */
[% endif %]

/* NOTE: not all databases support DROP TABLE IF EXISTS, you might need to
change this */
DROP TABLE IF EXISTS {{product}};

CREATE TABLE {{product}} AS
SELECT * FROM {{upstream['some_task']}};
