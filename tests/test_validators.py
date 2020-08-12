import pytest

from ploomber.validators import (data_frame_validator, validate_schema,
                                 validate_values, Assert)
import pandas as pd


def test_Assert():
    assert_ = Assert()

    assert_(False, 'Error message')
    assert_(True, 'Error message')

    assert len(assert_) == 1
    assert assert_.messages_error == ['Error message']
    assert repr(assert_) == 'Assert oject with 1 error messages'

    with pytest.raises(AssertionError) as excinfo:
        assert_.check()

    assert str(excinfo.value) == '1 error found: Error message'


def test_allows_optional_columns():
    df = pd.DataFrame({'a': [0], 'b': [0]})

    assert data_frame_validator(
        df, [validate_schema(schema={'a': 'int64'}, optional=['b'])])


def test_raises_on_unexpected_columns():
    df = pd.DataFrame({'a': [0], 'b': [0]})

    with pytest.raises(AssertionError):
        data_frame_validator(
            df,
            [
                validate_schema(schema={'a': 'int64'},
                                on_unexpected_cols='raise')
            ],
        )


def test_warns_on_unexpected_columns():
    df = pd.DataFrame({'a': [0], 'b': [0]})

    with pytest.warns(UserWarning):
        data_frame_validator(
            df,
            [
                validate_schema(schema={'a': 'int64'},
                                on_unexpected_cols='warn')
            ],
        )
