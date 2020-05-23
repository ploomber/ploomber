import pandas as pd

# + tags=["parameters"]
product = None
upstream = None

# +
df = pd.read_csv(str(upstream['load']['data']))
df = df + 1
df.to_csv(str(product['data']), index=False)
