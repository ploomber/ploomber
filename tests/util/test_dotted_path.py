from pathlib import Path
from unittest.mock import Mock

from test_pkg import functions
import pytest

from ploomber.util import dotted_path
from ploomber.exceptions import SpecValidationError
from ploomber.util import dotted_path


@pytest.mark.parametrize('spec', [
    'test_pkg.functions.some_function',
    {
        'dotted_path': 'test_pkg.functions.some_function'
    },
])
def test_call_spec(monkeypatch, spec):
    mock = Mock()
    monkeypatch.setattr(functions, 'some_function', mock)

    dotted_path.call_spec(spec)

    mock.assert_called_once_with()


def test_call_spec_with_kwargs(monkeypatch):
    mock = Mock()
    monkeypatch.setattr(functions, 'some_function', mock)

    spec = {
        'dotted_path': 'test_pkg.functions.some_function',
        'a': 1,
        'b': 2,
    }

    dotted_path.call_spec(spec)

    mock.assert_called_once_with(a=1, b=2)


def test_call_spec_without_dotted_path_key():
    spec = {'a': 1}

    with pytest.raises(SpecValidationError) as excinfo:
        dotted_path.call_spec(spec)

    assert excinfo.value.errors == [{
        'loc': ('dotted_path', ),
        'msg': 'field required',
        'type': 'value_error.missing'
    }]


@pytest.mark.parametrize('kwargs, expected', [
    [None, 42],
    [dict(a=1), 1],
])
def test_call_dotted_path(tmp_directory, add_current_to_sys_path,
                          no_sys_modules_cache, kwargs, expected):

    Path('my_module.py').write_text("""
def function(a=42):
    return a
""")

    assert dotted_path.call_dotted_path('my_module.function',
                                        kwargs=kwargs) == expected


def test_call_dotted_path_unexpected_kwargs(tmp_directory,
                                            add_current_to_sys_path,
                                            no_sys_modules_cache):

    Path('my_module.py').write_text("""
def function():
    pass
""")

    with pytest.raises(TypeError) as excinfo:
        dotted_path.call_dotted_path('my_module.function', kwargs=dict(a=1))

    expected = ("function() got an unexpected keyword argument 'a' "
                "(Loaded from:")
    assert expected in str(excinfo.value)
