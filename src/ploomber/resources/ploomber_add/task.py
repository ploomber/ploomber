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
# extract_upstream=True in your pipeline.yaml file, if this task has
# dependencies, list them them here (e.g. upstream = ['some_task']), otherwise
# leave as None
upstream = None
[% else -%]
# extract_upstream=False in your pipeline.yaml file, if this task has
# dependencies, declare them in the YAML spec and leave this as None. Values
# declared in the YAML spec will be added here during task execution
upstream = None
[% endif %]
[% if extract_product -%]
# extract_product=True in your pipeline.yaml file, product['nb'] must be the
# path to save the executed version of this task, other keys can be used
# to reference other output files
product = {'nb': 'products/notebook.ipynb', 'data': 'products/output.csv'}
[% else -%]
# extract_product=False in your pipeline.yaml file, leave this as None, the
# value in the YAML spec  will be added here during task execution
product = None
[% endif %]

# +
# your code here...
