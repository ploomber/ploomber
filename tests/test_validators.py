import numpy as np
import pandas as pd
import pytest

from ploomber.validators import (Assert, data_frame_validator, validate_schema,
                                 validate_values)


def test_Assert():
    assert_ = Assert()

    assert_(False, 'Error message')
    assert_(True, 'Another error message')

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


def test_validate_values_all_ok():
    df = pd.DataFrame({'z': [0, 1, 2], 'i': ['a', 'b', 'c']})

    data_frame_validator(df, [
        validate_values(values={
            'z': ('range', (0, 2)),
            'i': ('unique', {'a', 'b', 'c'}),
        })
    ])


def test_validate_values_invalid():
    df = pd.DataFrame({'z': [0, 1, 2], 'i': ['a', 'b', 'c']})

    with pytest.raises(AssertionError) as excinfo:
        data_frame_validator(
            df, [validate_values(values={
                'z': ('range', (0, 1)),
            })])

    expected = ('1 error found: validate_values: expected range of "z" to be '
                '[0, 1], got [0, 2]')
    assert expected == str(excinfo.value)
