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
    for name in ("get", "post", "put", "delete"):
        monkeypatch.setattr(
            _requests.requests,
            name,
            Mock(side_effect=requests.exceptions.ConnectionError),
        )


@pytest.mark.parametrize("method", methods)
def test_connection_error(method, mock_connection_error):
    with pytest.raises(exceptions.NetworkException) as excinfo:
        method("https://ploomber.io", json=dict(a=1))

    assert "connection error occurred" in str(excinfo.value)
    assert "https://ploomber.io" in str(excinfo.value)
    assert "https://ploomber.io/community" in str(excinfo.value)


@pytest.mark.parametrize(
    "method, method_name",
    [
        [_requests.get, "get"],
        [_requests.post, "post"],
        [_requests.delete, "delete"],
        [_requests.put, "put"],
    ],
)
def test_timeout_error(method, method_name, monkeypatch):
    mock = Mock(side_effect=requests.exceptions.Timeout)
    monkeypatch.setattr(_requests.requests, method_name, mock)

    with pytest.raises(exceptions.NetworkException) as excinfo:
        method("https://ploomber.io", timeout=1)

    mock.assert_called_once_with("https://ploomber.io", params=None, timeout=1)
    assert "server took too long" in str(excinfo.value)
    assert "https://ploomber.io" in str(excinfo.value)
    assert "https://ploomber.io/community" in str(excinfo.value)
