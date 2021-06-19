from pathlib import Path

import pytest

from ploomber.spec.lazycallable import LazyCallable
from ploomber.exceptions import CallbackSignatureError


def get_numbers(a=0, b=1):
    return 42, a, b


def get_more_numbers(a, b):
    return 42, a, b


@pytest.mark.parametrize('primitive, args, kwargs, out', [
    ['functions.get_numbers', [], {}, (42, 0, 1)],
    ['functions.get_numbers', [1, 2], {}, (42, 1, 2)],
    ['functions.get_numbers', [], {
        'a': 1,
        'b': 2
    }, (42, 1, 2)],
    ['functions.get_numbers', [1], {
        'b': 2
    }, (42, 1, 2)],
])
def test_lazy_callable_from_str(primitive, args, kwargs, out,
                                add_current_to_sys_path, no_sys_modules_cache,
                                tmp_directory):
    Path('functions.py').write_text("""
def get_numbers(a=0, b=1):
    return 42, a, b
""")

    callable_ = LazyCallable(primitive)
    assert callable_(*args, **kwargs) == out


def test_lazy_callable_from_str_does_not_import(add_current_to_sys_path,
                                                no_sys_modules_cache,
                                                tmp_directory):
    Path('functions.py').write_text("""
import unknown_package

def fn():
    pass
""")

    # ensure this doesnt exist
    try:
        import unknown_package  # noqa
    except ModuleNotFoundError:
        pass

    assert LazyCallable('functions.fn')


@pytest.mark.parametrize('args, kwargs, out', [
    [[], {}, (42, 0, 1)],
    [[1, 2], {}, (42, 1, 2)],
    [[], {
        'a': 1,
        'b': 2
    }, (42, 1, 2)],
    [[1], {
        'b': 2
    }, (42, 1, 2)],
])
def test_lazy_callable_from_callable(args, kwargs, out):

    callable_ = LazyCallable(get_numbers)
    assert callable_(*args, **kwargs) == out


def test_call_with_available():

    callable_ = LazyCallable(get_more_numbers)

    with pytest.raises(CallbackSignatureError):
        callable_.call_with_available(accepted_kwargs={'x': 1})
