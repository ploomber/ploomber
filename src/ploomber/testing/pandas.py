"""
Data testing using pandas. This is intended to be used by the spec API, if
using the Python API, it's better and more flexible to write your own
functions
"""
from pathlib import Path
import pandas as pd


def load_product(product):
    extension = Path(str(product)).suffix[1:]

    loader = getattr(pd, 'read_'+extension, None)

    if not loader:
        raise ValueError('Reading files with extension "{}" is currently '
                         'unsupported')
    else:
        return loader(product)


def no_nulls_in_columns(cols, product):
    return not nulls_in_columns(cols, product)


def nulls_in_columns(cols, product):
    """Check if any column has NULL values, returns bool
    """
    df = load_product(product)
    return df.isna().values.sum() > 0


def distinct_values_in_column(col, product):
    """Get distinct values in a column, returns a set
    """
    df = load_product(product)
    return set(df[col].unique())


def duplicates_in_column(col, product):
    """Check if a column has duplicated values, returns bool
    """
    df = load_product(product)
    return (df[col].value_counts() > 1).sum() > 0


def no_duplicates_in_column(col, product):
    return not duplicates_in_column(col, product)


def range_in_column(col, product):
    """Get range for a column, returns a (min_value, max_value) tuple
    """
    df = load_product(product)
    return df[col].min(), df[col].max()
