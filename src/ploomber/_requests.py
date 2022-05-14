"""
Internal module for making requests to external APIs. It wraps the requests
module to provide friendlier error messages and defaults
"""
import requests

from ploomber.exceptions import NetworkException


def _make_error(reason):
    return NetworkException(f'{reason}\n'
                            'If you need help, reach out to us: '
                            'https://ploomber.io/community')


def _request(method, url, params=None, **kwargs):
    try:
        response = method(url, params=params, **kwargs)
    except requests.exceptions.Timeout:
        # we do this instead of a chained exception since we want to hide
        # the original error and replace it with this one
        error = _make_error('The server took too long to respond '
                            f'when calling: {url}. Try again. ')
    except requests.exceptions.ConnectionError:
        error = _make_error('A connection '
                            f'error occurred when calling: {url}. '
                            'Verify your internet connection. ')
    else:
        error = None

    if error:
        raise error

    if response.status_code >= 300:
        json_ = response.json()
        message = json_.get("Message")

        if message:
            raise NetworkException(
                f'{message} (status: {response.status_code})')
        else:
            raise NetworkException(f'Error: {json_}')

    return response


def get(*args, **kwargs):
    return _request(requests.get, *args, **kwargs)


def post(*args, **kwargs):
    return _request(requests.post, *args, **kwargs)


def put(*args, **kwargs):
    return _request(requests.put, *args, **kwargs)


def delete(*args, **kwargs):
    return _request(requests.delete, *args, **kwargs)
