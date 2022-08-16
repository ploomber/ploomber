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

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# + tags=["parameters"]
upstream = {'clean': None}
product = 'output/plot.ipynb'

# +
df = pd.read_csv(str(upstream['clean']['data']))

plt.plot(np.random.rand(10))
