import pandas as pd


def serialize(value, product):
    value.to_parquet(str(product))


def unserialize(product):
    return pd.read_parquet(str(product))
