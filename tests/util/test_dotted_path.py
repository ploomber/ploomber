from unittest.mock import Mock

from test_pkg import functions
import pytest

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
