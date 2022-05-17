import inspect
from sklearn import datasets
from pathlib import Path
import pandas as pd

from ploomber.util.util import signature_check

def get(products):
    """Get data
    """
    d = datasets.load_iris()

    df = pd.DataFrame(d['data'])
    df.columns = d['feature_names']
    df['target'] = d['target']

    Path(str(product)).parent.mkdir(exist_ok=True, parents=True)

    df.to_parquet(str(product))

def test_signature_check():
    x = signature_check(get, inspect.signature(get).parameters, 'get')
    assert x == True