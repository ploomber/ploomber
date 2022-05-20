import inspect
import pytest
from sklearn import datasets
from pathlib import Path
import pandas as pd
from ploomber import dag
from ploomber.exceptions import TaskRenderError
from ploomber.sources.pythoncallablesource import PythonCallableSource
from ploomber.spec.dagspec import DAGSpec

from ploomber.util.util import signature_check

'''
def get(products):
    """Get data
    """
    d = datasets.load_iris()

    df = pd.DataFrame(d['data'])
    df.columns = d['feature_names']
    df['target'] = d['target']

    Path(str(product)).parent.mkdir(exist_ok=True, parents=True)

    df.to_parquet(str(product))
'''

def test_signature_check():
    
    spec1 = {
        'tasks': [{
            'source': 'tasks.get',
            'products': 'output/get.parquet',
        }]
    }

    error = ("You are passing products. Do you mean product?")

    x = PythonCallableSource(str(spec1))
    PythonCallableSource._post_render_validation(x, None, {'products': 1})
    
    #signature_check(get, {'product': 1}, 'get')      
    #assert error == str(e.value)
    