import pytest

from ploomber.validators import data_frame_validator
from ploomber.validators import validate_schema, validate_values
import pandas as pd


def test_allows_optional_columns():
    df = pd.DataFrame({'a': [0], 'b': [0]})

    assert data_frame_validator(df,
                                [validate_schema(schema={'a': 'int64'},
                                                 optional=['b'])])


def test_raises_on_unexpected_columns():
    df = pd.DataFrame({'a': [0], 'b': [0]})

    with pytest.raises(AssertionError):
        data_frame_validator(df,
                             [validate_schema(schema={'a': 'int64'},
                                              on_unexpected_cols='raise'
                                              )],)


def test_warns_on_unexpected_columns():
    df = pd.DataFrame({'a': [0], 'b': [0]})

    with pytest.warns(UserWarning):
        data_frame_validator(df,
                             [validate_schema(schema={'a': 'int64'},
                                              on_unexpected_cols='warn'
                                              )],)
