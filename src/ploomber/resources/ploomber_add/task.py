# ---
# jupyter:
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# Add description here
#
[% if not is_ipynb -%]
# *Note:* You can open this file as a notebook (JupyterLab: right-click on it in the side bar -> Open With -> Notebook)
[% endif %]

# +
# Uncomment the next two lines to enable auto reloading for imported modules
# # %load_ext autoreload
# # %autoreload 2
# For more info, see:
# https://ploomber.readthedocs.io/en/latest/user-guide/faq_index.html#auto-reloading-code-in-jupyter

# + tags=["parameters"]
[% if extract_upstream -%]
# extract_upstream=True in your pipeline.yaml file, if this task has
# dependencies, list them them here (e.g. upstream = ['some_task']), otherwise
# leave as None. Once you modify the variable, reload it for Ploomber to inject
# the cell (On JupyterLab: File -> Reload File from Disk)
upstream = None
[% else -%]
# extract_upstream=False in your pipeline.yaml file, if this task has
# dependencies, declare them in the YAML spec and leave this as None. Once you
# add the dependencies, reload the file for Ploomber to inject the cell
# (On JupyterLab: File -> Reload File from Disk)
upstream = None
[% endif %]
[% if extract_product -%]
# extract_product=True in your pipeline.yaml file, product['nb'] must be the
# path to save the executed version of this task, other keys can be used
# to reference other output files. A cell must be injected below this one,
# if you don't see it, check the Jupyter logs
product = {'nb': 'products/notebook.ipynb'}
[% else -%]
# extract_product=False in your pipeline.yaml file, leave this as None, the
# value in the YAML spec will be injected in a cell below. If you don't see it,
# check the Jupyter logs
product = None
[% endif %]

# +
# your code here...
