# ---
# jupyter:
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# Add description here

# + tags=["parameters"]
[% if extract_upstream -%]
# if this task has upstream dependencies, list their names here. If there are
# no dependencies, make it equal to None
upstream = {'some_task', 'another_task'}
[% else %]
# extract_upstream is set to False in your pipeline.yaml, keep it equal to
# None here and declare "upstream" dependencies directly in the YAML spec
upstream = None
[% endif %]
[% if extract_product -%]
# path to save the executed version of this script (a notebook), if this task
# generates other files, declare a dictionary with a key "nb" to point to the
# executed notebook and any other keys for other files
product = {'nb': 'path/to/notebook.ipynb', 'data': 'path/to/output.csv'}
[% else %]
# extract_product is set to False in your pipeline.yaml, keep it equal to
# None here and declare a "product" directly in the YAML spec
upstream = None
[% endif %]
# NOTE: During execution, a cell will be injected below to resolve "upstream"
# and "product" based on preferences in your pipeline.yaml spec. If you
# activated the jupyter notebook extension, you'll see the injected cell
# as well when you open this file from the jupyter notebook app

# +
# your code here...
