/* Add description for your SQL task here

*/

[% if extract_upstream -%]
/* NOTE: extract_upstream is set to True in your pipeline.yaml file,
declare "upstream" dependencies by using the "upstream" dictionary
*/
[% else %]
/* NOTE: extract_upstream is set to False in your pipeline.yaml file,
declare "upstream" dependencies directly in the YAML spec. References
in this file must match
*/
[% endif %]

[% if extract_product -%]
/* NOTE: extract_product is set to True in your pipeline.yaml file,
declare "product" here as a variable and reference it as {{product}}
*/
[% else %]
/* NOTE: extract_product is set to False in your pipeline.yaml file,
declare "product" in the YAML spec and reference it here as {{product}}
*/
[% endif %]

-- NOTE: not all databases support DROP TABLE IF EXISTS
DROP TABLE IF EXISTS {{product}};

CREATE TABLE AS {{product}}
SELECT * FROM {{upstream['some_task']}};