from pathlib import Path
import pandas as pd
import numpy as np


def get(product):
    """Get data
    """
    df = pd.DataFrame(np.random.rand(100, 10))
    df.columns = [str(i) for i in range(10)]

    Path(str(product)).parent.mkdir(exist_ok=True, parents=True)
    df.to_parquet(str(product))


def features(upstream, product):
    """Generate new features from existing columns
    """
    data = pd.read_parquet(str(upstream['get']))
    ft = data['1'] * data['2']
    df = pd.DataFrame({'feature': ft, 'another': ft**2})
    df.to_parquet(str(product))


def join(upstream, product):
    """Join raw data with generated features
    """
    a = pd.read_parquet(str(upstream['get']))
    b = pd.read_parquet(str(upstream['features']))
    df = a.join(b)
    df.to_parquet(str(product))
