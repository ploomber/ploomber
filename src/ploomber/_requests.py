"""
Internal module for making requests to external APIs. It wraps the requests
module to provide friendlier error messages and defaults
"""
import json
from json import JSONDecodeError

import requests

from ploomber.exceptions import NetworkException, RawBaseException


def _make_error(reason):
    return NetworkException(
        f"{reason}\n"
        "If you need help, reach out to us: "
        "https://ploomber.io/community"
    )


def _request(method, url, params=None, json_error=False, **kwargs):
    try:
        response = method(url, params=params, **kwargs)
    except requests.exceptions.Timeout:
        # we do this instead of a chained exception since we want to hide
        # the original error and replace it with this one
        error = _make_error(
            "The server took too long to respond " f"when calling: {url}. Try again. "
        )
    except requests.exceptions.ConnectionError:
        error = _make_error(
            "A connection "
            f"error occurred when calling: {url}. "
            "Verify your internet connection. "
        )
    else:
        error = None

    if error:
        raise error

    if response.status_code >= 300:
        try:
            json_ = response.json()
        except JSONDecodeError:
            json_ = None

        if json_ is None:
            raise NetworkException(
                f"An error happened (code: {response.status_code})",
                code=response.status_code,
            )

        message = json_.get("Message")

        if message and not json_error:
            raise NetworkException(
                f"{message} (status: {response.status_code})", code=response.status_code
            )
        elif not message and not json_error:
            raise NetworkException(f"Error: {json_}", code=response.status_code)
        else:
            raise RawBaseException(json.dumps(json_))

    return response


def get(*args, **kwargs):
    return _request(requests.get, *args, **kwargs)


def post(*args, json_error=False, **kwargs):
    return _request(requests.post, *args, **kwargs, json_error=json_error)


def put(*args, **kwargs):
    return _request(requests.put, *args, **kwargs)


def delete(*args, **kwargs):
    return _request(requests.delete, *args, **kwargs)
