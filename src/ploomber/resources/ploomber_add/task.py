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

# %%
# Uncomment the next two lines to enable auto reloading for imported modules
# # %load_ext autoreload
# # %autoreload 2
# For more info, see:
# https://docs.ploomber.io/en/latest/user-guide/faq_index.html#auto-reloading-code-in-jupyter

# %% tags=["parameters"]
[% if extract_upstream -%]
# If this task has dependencies, list them them here
# (e.g. upstream = ['some_task']), otherwise leave as None.
upstream = None
[% else -%]
# If this task has dependencies, declare them in the YAML spec and leave this
# as None
upstream = None
[% endif %]
[% if extract_product -%]
# product['nb'] must be the path to save the executed version of this task,
# other keys can be used to reference other output files
product = {'nb': 'products/notebook.ipynb'}
[% else -%]
# This is a placeholder, leave it as None
product = None
[% endif %]

# %%
# your code here...
