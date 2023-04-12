import numpy as np
import pandas as pd
from ploomber.testing import pandas as pd_t


def test_nulls_in_columns(tmp_directory):
    df = pd.DataFrame({"x": np.random.rand(10), "y": np.random.rand(10)})
    df.to_csv("data.csv", index=False)

    assert not pd_t.nulls_in_columns(["x", "y"], "data.csv")

    df = pd.DataFrame({"x": np.random.rand(10), "y": np.random.rand(10)})
    df.iloc[0, 0] = np.nan
    df.to_csv("data.csv", index=False)

    assert pd_t.nulls_in_columns(["x", "y"], "data.csv")


def test_distinct_values_in_column(tmp_directory):
    df = pd.DataFrame({"x": range(5)})
    df.to_csv("data.csv", index=False)

    assert pd_t.distinct_values_in_column("x", "data.csv") == set(range(5))


def test_duplicates_in_column(tmp_directory):
    df = pd.DataFrame({"x": np.random.randint(0, 5, size=10)})
    df.to_csv("data.csv", index=False)

    assert pd_t.duplicates_in_column("x", "data.csv")

    df = pd.DataFrame({"x": range(10)})
    df.to_csv("data.csv", index=False)

    assert not pd_t.duplicates_in_column("x", "data.csv")


def test_range_in_column(tmp_directory):
    df = pd.DataFrame({"x": range(5)})
    df.to_csv("data.csv", index=False)

    assert pd_t.range_in_column("x", "data.csv") == (0, 4)
