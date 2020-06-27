import numpy as np
import pandas as pd

# + tags=["parameters"]
upstream = None
product = None

# +
df = pd.DataFrame({'x': np.random.rand(100), 'y': np.random.rand(100)})
df.to_csv(str(product['data']), index=False)
