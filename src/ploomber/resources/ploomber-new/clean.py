import numpy as np
import pandas as pd

# + tags=["parameters"]
upstream = None
product = {'nb': 'output/clean.ipynb', 'data': 'output/clean.csv'}

# +
df = pd.DataFrame({'x': np.random.rand(100), 'y': np.random.rand(100)})
df.head()

# +
df.to_csv(str(product['data']), index=False)
