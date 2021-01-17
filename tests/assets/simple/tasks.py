from pathlib import Path
import pandas as pd
from sklearn import datasets


def get(product):
    """Get data
    """
    d = datasets.load_iris()

    df = pd.DataFrame(d['data'])
    df.columns = d['feature_names']
    df['target'] = d['target']

    Path(str(product)).parent.mkdir(exist_ok=True, parents=True)

    df.to_parquet(str(product))


def features(upstream, product):
    """Generate new features from existing columns
    """
    data = pd.read_parquet(str(upstream['get']))
    ft = data['sepal length (cm)'] * data['sepal width (cm)']
    df = pd.DataFrame({'sepal area (cm2)': ft, 'another': ft**2})
    df.to_parquet(str(product))


def join(upstream, product):
    """Join raw data with generated features
    """
    a = pd.read_parquet(str(upstream['get']))
    b = pd.read_parquet(str(upstream['features']))
    c = pd.read_parquet(str(upstream['more_features']))
    df = a.join(b).join(c)
    df.to_parquet(str(product))


def more_features(product, upstream):
    """Add description here
    """
    data = pd.read_parquet(str(upstream['get']))
    sepal_l_plus_w = data['sepal length (cm)'] + data['sepal width (cm)']
    out = pd.DataFrame({'sepal length + width': sepal_l_plus_w})
    out.to_parquet(str(product))