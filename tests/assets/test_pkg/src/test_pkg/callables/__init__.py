import pandas as pd


def root(product):
    df = pd.DataFrame({'x': [1, 2, 3]})
    df.to_csv(str(product))


def a(upstream, product):
    df = pd.read_csv(str(upstream['root']))
    (df + 1).to_csv(str(product))


def b(upstream, product):
    df = pd.read_csv(str(upstream['root']))
    (df + 10).to_csv(str(product))
