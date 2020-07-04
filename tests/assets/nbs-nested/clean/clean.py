import os
import some_functions
import pandas as pd

# +
print(os.getcwd())

# + tags=["parameters"]
upstream = {'load': None}
product = {'nb': '../output/clean.ipynb', 'data': '../output/clean.csv'}

# +
some_functions.some_print()

# +
df = pd.read_csv(str(upstream['load']['data']))
df = df + 1
df.to_csv(str(product['data']), index=False)
