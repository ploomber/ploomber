import pandas as pd
import matplotlib.pyplot as plt

# + tags=["parameters"]
upstream = {'clean': None}
product = None

# +
df = pd.read_csv(str(upstream['clean']['data']))


plt.scatter(df.x, df.y)
