# ---
# jupyter:
#   jupytext:
#     notebook_metadata_filter: ploomber
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
#   ploomber: {}
# ---

import pandas as pd

# + tags=["parameters"]
upstream = {'load': None}
product = {'nb': 'output/clean.ipynb', 'data': 'output/clean.csv'}
# -

df = pd.read_csv(str(upstream['load']['data']))
df = df + 1
df.to_csv(str(product['data']), index=False)
