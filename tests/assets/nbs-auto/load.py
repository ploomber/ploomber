import pandas as pd
import numpy as np

# + tags=["parameters"]
upstream = None
product = {'nb': 'output/load.ipynb', 'data': 'output/data.csv'}

# +
data = np.random.rand(1000, 10)
df = pd.DataFrame(data)
df.to_csv(str(product['data']), index=False)
