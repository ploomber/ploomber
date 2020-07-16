"""
Generate plot
"""
import pandas as pd

# + tags=["parameters"]
upstream = ['clean']
product = {'nb': 'output/plot.ipynb'}
# -

# +
df = pd.read_csv(upstream['clean']['data'])
grouped = df.groupby('sex')[['age', 'hours-per-week']].mean()
grouped.columns = ['Mean age', 'Mean hours per week worked']
grouped.head()
# -

ax = grouped.plot.barh()
# -
