import inspect
import pytest
from sklearn import datasets
from pathlib import Path
import pandas as pd
from ploomber.exceptions import TaskRenderError

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
    error = ("You are passing products. Do you mean product?")

    with pytest.raises(TaskRenderError) as e:
        signature_check(get, {'product': 1}, 'get')
        
    assert error == str(e.value)