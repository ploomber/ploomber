import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# + tags=["parameters"]
upstream = {'clean.py': None}
product = None

# +
df = pd.read_csv(str(upstream['clean.py']['data']))


plt.plot(np.random.rand(10))
