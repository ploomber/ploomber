"""
Get the raw data and save it

Data reference: https://archive.ics.uci.edu/ml/datasets/Adult
"""
import pandas as pd

# + tags=["parameters"]
upstream = None
product = {'nb': 'output/data.ipynb', 'data': 'output/data.csv'}
# -

# +
url = ('https://archive.ics.uci.edu/ml/machine-learning-databases/adult'
       '/adult.data')
df = pd.read_csv(url, header=None)
df.columns = [
    'age', 'workclass', 'fnlwgt', 'education', 'education-num',
    'marital-status', 'occupation', 'relationship', 'race', 'sex',
    'capital-gain', 'capital-loss', 'hours-per-week', 'native-country',
    'income'
]
df.head()
# -

# +
df.to_csv(str(product['data']), index=False)
# -
