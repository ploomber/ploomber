"""
https://docs.python-requests.org/en/latest/api/#exceptions
"""
from unittest.mock import Mock

import pytest
import requests

from ploomber import _requests, exceptions

methods = [
    _requests.get,
    _requests.post,
    _requests.delete,
    _requests.put,
]


@pytest.fixture
def mock_connection_error(monkeypatch):
    for name in ('get', 'post', 'put', 'delete'):
        monkeypatch.setattr(
            _requests.requests, name,
            Mock(side_effect=requests.exceptions.ConnectionError))


@pytest.mark.parametrize('method', methods)
def test_connection_error(method, mock_connection_error):
    with pytest.raises(exceptions.NetworkException) as excinfo:
        method('https://ploomber.io', json=dict(a=1))

    assert 'connection error occurred' in str(excinfo.value)
    assert 'https://ploomber.io' in str(excinfo.value)
    assert 'https://ploomber.io/community' in str(excinfo.value)


@pytest.mark.parametrize('method', methods)
def test_timeout_error(method):
    with pytest.raises(exceptions.NetworkException) as excinfo:
        method('https://ploomber.io', timeout=0.0001)

    assert 'server took too long' in str(excinfo.value)
    assert 'https://ploomber.io' in str(excinfo.value)
    assert 'https://ploomber.io/community' in str(excinfo.value)
