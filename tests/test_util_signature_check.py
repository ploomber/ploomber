import pytest
from ploomber.util.util import callback_check
from ploomber.exceptions import CallbackSignatureError


def test_fn_with_default_values():
    def fn(a, b, default=1):
        pass

    with pytest.raises(CallbackSignatureError):
        callback_check(fn, {'a', 'b'})


def test_fn_with_unknown_params():
    def fn(a, b, unknown):
        pass

    with pytest.raises(CallbackSignatureError):
        callback_check(fn, {'a', 'b'})


def test_returns_kwargs_to_use():
    def fn(a, b):
        pass

    assert callback_check(fn, {'a': 1, 'b': 2, 'c': 3}) == {'a': 1, 'b': 2}
