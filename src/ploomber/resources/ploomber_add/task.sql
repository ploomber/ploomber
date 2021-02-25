/* Add description here
*/

[% if extract_upstream -%]
/* extract_upstream=True in your pipeline.yaml file, if this task has
upstream dependencies, declare them here */
[% else %]
/* extract_upstream=False in your pipeline.yaml file, if this task has
upstream dependencies, declare them the YAML spec and reference them here */
[% endif %]

[% if extract_product -%]
/* extract_product=True in your pipeline.yaml file, declare a "product"
variable in this file */
[% endif %]

/* NOTE: not all databases support DROP TABLE IF EXISTS, you might need to
change this */
DROP TABLE IF EXISTS {{product}};

CREATE TABLE {{product}} AS
SELECT * FROM {{upstream['some_task']}};
