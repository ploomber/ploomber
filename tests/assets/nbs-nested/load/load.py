import os
import pandas as pd
import numpy as np
import load_util

# +
print(os.getcwd())

# +
load_util.a_function()

# + tags=["parameters"]
upstream = None
product = {"nb": "../output/load.ipynb", "data": "../output/data.csv"}

# +
data = np.random.rand(1000, 10)
df = pd.DataFrame(data)
df.to_csv(str(product["data"]), index=False)
