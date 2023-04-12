import pandas as pd
import pytest

from ploomber.validators import (
    Assert,
    data_frame_validator,
    validate_schema,
    validate_values,
)
from ploomber.validators import string


def test_Assert():
    assert_ = Assert()

    assert_(False, "Error message")
    assert_(True, "Another error message")

    assert len(assert_) == 1
    assert assert_.messages_error == ["Error message"]
    assert repr(assert_) == "Assert oject with 1 error messages"

    with pytest.raises(AssertionError) as excinfo:
        assert_.check()

    assert str(excinfo.value) == "1 error found:\nError message"


@pytest.fixture
def assert_():
    assert_ = Assert()
    assert_(False, "1")
    assert_(False, "2")
    assert_(False, "3")
    return assert_


def test_Assert_iter(assert_):
    assert list(assert_) == ["1", "2", "3"]


def test_Assert_str_without_errors():
    assert str(Assert()) == "No errors found"


def test_Assert_str_with_errors(assert_):
    assert "3 errors found" in str(assert_)
    assert all(msg in str(assert_) for msg in ("1", "2", "3"))


def test_Assert_with_warning(assert_):
    assert_.warn(False, "4")

    assert "3 errors found" in str(assert_)
    assert all(msg in str(assert_) for msg in ("1", "2", "3"))

    assert "1 warning" in str(assert_)
    assert "4" in str(assert_)


def test_Assert_with_warnings(assert_):
    assert_.warn(False, "4")
    assert_.warn(False, "5")

    assert "3 errors found" in str(assert_)
    assert all(msg in str(assert_) for msg in ("1", "2", "3"))

    assert "2 warnings" in str(assert_)
    assert all(msg in str(assert_) for msg in ("4", "5"))


def test_allows_optional_columns():
    df = pd.DataFrame({"a": [0], "b": [0]})

    assert data_frame_validator(
        df, [validate_schema(schema={"a": "int64"}, optional=["b"])]
    )


def test_validates_optional_schema():
    df = pd.DataFrame({"a": [0], "b": [0]})

    with pytest.raises(AssertionError):
        data_frame_validator(
            df, [validate_schema(schema={"a": "int64"}, optional={"b": "object"})]
        )


def test_ignores_dtype_validation_if_none():
    df = pd.DataFrame({"a": [0], "b": [0]})

    data_frame_validator(
        df, [validate_schema(schema={"a": None}, optional={"b": None})]
    )


def test_raises_on_unexpected_columns():
    df = pd.DataFrame({"a": [0], "b": [0]})

    with pytest.raises(AssertionError):
        data_frame_validator(
            df,
            [validate_schema(schema={"a": "int64"}, on_unexpected_cols="raise")],
        )


def test_warns_on_unexpected_columns():
    df = pd.DataFrame({"a": [0], "b": [0]})

    with pytest.warns(UserWarning):
        data_frame_validator(
            df,
            [validate_schema(schema={"a": "int64"}, on_unexpected_cols="warn")],
        )


def test_validate_values_all_ok():
    df = pd.DataFrame({"z": [0, 1, 2], "i": ["a", "b", "c"]})

    data_frame_validator(
        df,
        [
            validate_values(
                values={
                    "z": ("range", (0, 2)),
                    "i": ("unique", {"a", "b", "c"}),
                }
            )
        ],
    )


def test_validate_values_invalid():
    df = pd.DataFrame({"z": [0, 1, 2], "i": ["a", "b", "c"]})

    with pytest.raises(AssertionError) as excinfo:
        data_frame_validator(
            df,
            [
                validate_values(
                    values={
                        "z": ("range", (0, 1)),
                    }
                )
            ],
        )

    expected = (
        "Data frame validation failed. 1 error found:\n(validate_values) "
        'Expected range of "z" to be [0, 1], got [0, 2]'
    )
    assert expected == str(excinfo.value)


# Validating task and product strings


def test_error_on_invalid_task_class_name():
    with pytest.raises(ValueError) as excinfo:
        string.validate_task_class_name("invalid_class")

    assert str(excinfo.value) == "'invalid_class' is not a valid Task class name"


@pytest.mark.parametrize(
    "name, expected",
    [
        ["sqlscript", "SQLScript"],
        ["SQLSCRIPT", "SQLScript"],
        ["sql_script", "SQLScript"],
        ["sql-script", "SQLScript"],
        ["sql script", "SQLScript"],
        ["sqldump", "SQLDump"],
        ["aslscript", "SQLScript"],
        ["sqLscrip", "SQLScript"],
    ],
)
def test_error_on_invalid_task_class_name_with_typo(name, expected):
    with pytest.raises(ValueError) as excinfo:
        string.validate_task_class_name(name)

    expected = (
        f"{name!r} is not a valid Task class name. " f"Did you mean {expected!r}?"
    )
    assert str(excinfo.value) == expected


@pytest.mark.parametrize(
    "name, expected",
    [
        ["file", "File"],
        ["FILE", "File"],
        ["fi_le", "File"],
        ["fi-le", "File"],
        ["fi le", "File"],
        ["filee", "File"],
        ["FiL", "File"],
        ["sqlrelation", "SQLRelation"],
    ],
)
def test_error_on_invalid_product_class_name_with_typo(name, expected):
    with pytest.raises(ValueError) as excinfo:
        string.validate_product_class_name(name)

    expected = (
        f"{name!r} is not a valid Product class name. " f"Did you mean {expected!r}?"
    )
    assert str(excinfo.value) == expected
