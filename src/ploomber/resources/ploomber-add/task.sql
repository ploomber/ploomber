/* Add description here

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

-- NOTE: not all databases support DROP TABLE IF EXISTS
DROP TABLE IF EXISTS {{product}};

CREATE TABLE AS {{product}}
SELECT * FROM {{upstream['some_task']}};