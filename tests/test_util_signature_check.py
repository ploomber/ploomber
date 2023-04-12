from pathlib import Path

import pytest
from ploomber.util.util import callback_check, signature_check
from ploomber.util.dotted_path import DottedPath
from ploomber.exceptions import (
    CallbackSignatureError,
    CallbackCheckAborted,
    TaskRenderError,
)


def test_fn_with_default_values():
    def fn(a, b, default=1):
        pass

    with pytest.raises(CallbackSignatureError):
        callback_check(fn, {"a", "b"}, allow_default=False)


def test_fn_with_unknown_params():
    def fn(a, b, unknown):
        pass

    with pytest.raises(CallbackSignatureError):
        callback_check(fn, {"a", "b"})


def test_returns_kwargs_to_use():
    def fn(a, b):
        pass

    assert callback_check(fn, {"a": 1, "b": 2, "c": 3}) == {"a": 1, "b": 2}


def test_callback_check_from_dotted_path(tmp_directory, tmp_imports):
    Path("some_module.py").write_text(
        """
def fn(some_arg):
    return some_arg
"""
    )

    dp = DottedPath("some_module.fn", lazy_load=False)

    assert callback_check(dp, available={"some_arg": 42}) == {"some_arg": 42}


def test_error_if_lazy_loaded_dotted_path():
    dp = DottedPath("not_a_module.not_a_function", lazy_load=True)

    with pytest.raises(CallbackCheckAborted):
        callback_check(dp, available={"some_arg": 42})


def test_no_error_if_args_passed_to_the_constructor(tmp_directory, tmp_imports):
    Path("some_module.py").write_text(
        """
def fn(some_arg, another_arg):
    return some_arg
"""
    )

    dp = DottedPath(dict(dotted_path="some_module.fn", some_arg=42), lazy_load=False)

    assert callback_check(dp, available={"another_arg": 100}) == {"another_arg": 100}


def test_overrides_default_param_with_available_param(tmp_directory, tmp_imports):
    Path("some_module.py").write_text(
        """
def fn(some_arg, another_arg):
    return some_arg
"""
    )

    dp = DottedPath(dict(dotted_path="some_module.fn", some_arg=42), lazy_load=False)

    assert callback_check(dp, available={"another_arg": 100, "some_arg": 200}) == {
        "another_arg": 100,
        "some_arg": 200,
    }


def test_callback_check_doesnt_include_constructor_args(tmp_directory, tmp_imports):
    Path("some_module.py").write_text(
        """
def add(x, y):
    return x + y
"""
    )

    dp = DottedPath(dict(dotted_path="some_module.add", x=1), lazy_load=False)

    assert callback_check(dp, available={"y": 2}) == {"y": 2}
    assert dp(y=2) == 3


def test_signature_check_extra():
    def fn():
        pass

    with pytest.raises(TaskRenderError) as excinfo:
        signature_check(fn, {"b": 1}, "task")

    error = (
        'Error rendering task "task" initialized with function "fn". '
        "Got unexpected arguments: ['b']"
    )
    assert error == str(excinfo.value)


def test_signature_check_params_missing():
    def fn(a, b):
        pass

    with pytest.raises(TaskRenderError) as excinfo:
        signature_check(fn, {"b": 1}, "task")

    error = (
        'Error rendering task "task" initialized with function '
        "\"fn\". Missing arguments: ['a']. "
        "Pass ['a'] in \"params\""
    )
    assert error == str(excinfo.value)


def test_signature_check_upstream_missing():
    def fn(upstream):
        pass

    with pytest.raises(TaskRenderError) as excinfo:
        signature_check(fn, dict(), "task")

    error = (
        'Error rendering task "task" initialized with function '
        "\"fn\". Missing arguments: ['upstream']. "
        "Verify this task declared upstream depedencies or remove the "
        '"upstream" argument from the function'
    )
    assert error == str(excinfo.value)


def test_signature_check_params_and_upstream_missing():
    def fn(upstream, a):
        pass

    with pytest.raises(TaskRenderError) as excinfo:
        signature_check(fn, dict(), "task")

    error = (
        'Error rendering task "task" initialized with function "fn". '
        "Missing arguments: ['a', 'upstream']. Verify this "
        'task declared upstream depedencies or remove the "upstream" '
        "argument from the function. "
        "Pass ['a'] in \"params\""
    )
    assert error == str(excinfo.value)


def test_signature_check_both():
    def fn(a):
        pass

    with pytest.raises(TaskRenderError) as excinfo:
        signature_check(fn, {"b": 1}, "task")

    error = (
        'Error rendering task "task" initialized with function '
        "\"fn\". Got unexpected arguments: ['b']. "
        "Missing arguments: ['a']. Pass ['a'] in \"params\""
    )

    assert error == str(excinfo.value)


def test_signature_check_ignores_params_w_default():
    def fn(a, b=1):
        pass

    assert signature_check(fn, {"a": 1}, "task")
