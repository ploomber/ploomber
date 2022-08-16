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
import numpy as np

# + tags=["parameters"]
upstream = None
product = {'nb': 'output/load.ipynb', 'data': 'output/data.csv'}
# -

data = np.random.rand(1000, 10)
df = pd.DataFrame(data)
df.to_csv(str(product['data']), index=False)
