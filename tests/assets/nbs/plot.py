import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# + tags=["parameters"]
upstream = {"clean": None}
product = "output/plot.ipynb"

# +
df = pd.read_csv(str(upstream["clean"]["data"]))


plt.plot(np.random.rand(10))
